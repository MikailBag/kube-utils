//! Distributed locks on top of k8s.
//! This lock does not guarantee mutual exclustion.
//! In rare conditions several pods can hold this lock in the same time.
//! That's why it should be only used as an optimization.

use crate::errors::{classify_kube, ErrorClass};
use anyhow::Context as _;
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::MicroTime,
};
use kube::{
    api::{DeleteParams, ObjectMeta, PostParams, Preconditions, ResourceExt},
    Api,
};
use rand::distributions::Distribution;
use serde::{Deserialize, Serialize};
use std::{
    convert::TryInto,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

const DEFAULT_LOCK_TIMEOUT: u64 = 30;
const DEFAULT_RENEW_INTERVAL: u64 = 5;
const DEFAULT_CONFLICT_SKIP_MILLIS: u128 = 100;

#[derive(Debug)]
pub struct LockParams {
    /// Namespace of the lease to use
    pub namespace: String,
    /// Name of the lease to use
    pub name: String,
    /// Override lock renew inteval (i.e. how often we should update map
    /// to show that we are still alive). Default is 5 seconds.
    /// Rounded down to seconds.
    pub lock_renew_interval: Option<Duration>,
    /// Duration lock will be valid. It must be bigger than lock_renew_interval.
    pub lock_timeout: Option<Duration>,
    /// Duration to sleep in case of conflict.
    /// Actually it will be multiplied by a random number
    /// in range [1, 1.1]
    pub conflict_sleep: Option<Duration>,
}

impl LockParams {
    fn lock_renew_interval(&self) -> Duration {
        self.lock_renew_interval
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_RENEW_INTERVAL))
    }

    fn lock_timeout(&self) -> Duration {
        self.lock_timeout
            .unwrap_or_else(|| Duration::from_secs(DEFAULT_LOCK_TIMEOUT))
    }
}

#[derive(Serialize, Deserialize)]
struct LockContent {
    data: serde_json::Value,
}

impl LockContent {
    fn from_data(data: serde_json::Value) -> Self {
        LockContent {
            data,
            //uid: uuid::Uuid::new_v4(),
        }
    }
}

fn read_lock_content<'a>(p: &'a Lease) -> Option<&'a str> {
    p.spec
        .as_ref()
        .and_then(|spec| spec.holder_identity.as_deref())
}

/// Type that represents successfully acquired lock.
/// On drop it will try to release the lock (so that other pod
/// can instantly become a new leader). If it is impossible (e.g. pod
/// aborts, or is forcefully killed by the OOMKiller, or the guard is leaked,
/// or if API requests fail) other pod will acquire the lock only when
/// timeout expires.
pub struct LockGuard {
    /// Real lock guard implementation lives in the background tasks.
    /// We use this cancellation token to signal that lock should
    /// be released.
    token: CancellationToken,
}

fn make_lease(
    prev_lease: Option<&Lease>,
    lease_name: &str,
    user_data: &serde_json::Value,
    lock_timeout: Duration,
) -> Lease {
    let holder_identity = serde_json::to_string(&LockContent::from_data(user_data.clone()))
        .expect("failed to serialize LockContent");
    let rounded_lock_timeout = lock_timeout.as_secs().clamp(1, i32::max_value() as u64) as i32;

    Lease {
        metadata: ObjectMeta {
            name: Some(lease_name.to_string()),
            resource_version: prev_lease.and_then(|lease| lease.metadata.resource_version.clone()),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            acquire_time: Some(MicroTime(chrono::Utc::now())),
            holder_identity: Some(holder_identity),
            lease_duration_seconds: Some(rounded_lock_timeout),
            renew_time: Some(MicroTime(chrono::Utc::now())),
            lease_transitions: None,
        }),
    }
}

fn make_post_params() -> PostParams {
    PostParams {
        field_manager: Some("kube-utils".to_string()),
        ..Default::default()
    }
}

enum TryReplaceOutcome {
    Success(Lease),
    Conflict,
}

async fn try_replace_lease(
    api: &Api<Lease>,
    prev: Option<&Lease>,
    mut new: Lease,
) -> anyhow::Result<TryReplaceOutcome> {
    match prev {
        Some(prev) => {
            new.metadata.resource_version = prev.metadata.resource_version.clone();
            match api.replace(&new.name(), &make_post_params(), &new).await {
                Ok(created) => Ok(TryReplaceOutcome::Success(created)),
                Err(err) => match classify_kube(&err) {
                    ErrorClass::Conflict => Ok(TryReplaceOutcome::Conflict),
                    _ => Err(err).context("failed to replace old lease"),
                },
            }
        }
        None => match api.create(&make_post_params(), &new).await {
            Ok(created) => Ok(TryReplaceOutcome::Success(created)),
            Err(err) => match classify_kube(&err) {
                ErrorClass::AlreadyExists => Ok(TryReplaceOutcome::Conflict),
                _ => Err(err).context("failed to create lease"),
            },
        },
    }
}

impl LockGuard {
    fn new(
        api: Api<Lease>,
        name: &str,
        params: &LockParams,
        user_data: serde_json::Value,
        mut prev_lease: Lease,
    ) -> LockGuard {
        let token = CancellationToken::new();

        let last_lease = Arc::new(Mutex::new(prev_lease.clone()));
        // task that renews lock.
        {
            let token = token.clone();
            let renew_interval = params.lock_renew_interval();
            let lock_timeout = params.lock_timeout();
            let name = name.to_string();
            let api = api.clone();
            let last_lease = last_lease.clone();

            let task_renew_lock = async move {
                loop {
                    tokio::time::sleep(renew_interval).await;
                    tracing::debug!("Renewing lock");
                    let lease = make_lease(Some(&prev_lease), &name, &user_data, lock_timeout);

                    match api.replace(&name, &make_post_params(), &lease).await {
                        Ok(new_lease) => {
                            prev_lease = new_lease.clone();
                            *last_lease.lock().unwrap() = new_lease;
                        }
                        Err(e) => {
                            tracing::warn!("Failed to renew lock: {:#}", e);
                        }
                    }
                }
            }
            .in_current_span();

            tokio::task::spawn(async move {
                tokio::select! {
                    _ = task_renew_lock => {},
                    _ = token.cancelled() => {},
                }
                tracing::info!("Got cancellation request, stopped renewing lock");
            });
        }
        // task that tries to revoke the lock.
        {
            let token = token.clone();
            let name = name.to_string();
            tokio::task::spawn(async move {
                token.cancelled().await;
                tracing::info!("Releasing lock");
                let mut delete_params = DeleteParams::default();
                let last_lease = { last_lease.lock().unwrap().clone() };

                delete_params.preconditions = Some(Preconditions {
                    resource_version: last_lease.metadata.resource_version.clone(),
                    uid: last_lease.metadata.uid.clone(),
                });

                if let Err(e) = api.delete(&name, &delete_params).await {
                    tracing::warn!("Failed to gracefully release the lock: {:#}", e);
                }
            });
        }
        LockGuard { token }
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        tracing::info!("LockGuard is dropped, releasing the lock");
        self.token.cancel();
    }
}

pub enum LockResult {
    /// Lock was acquired successfully. Returned `LockGuard` should be kept
    /// as long as calling process wants to remain leader.
    Leader(LockGuard),
    /// Lock is acquired by other actor which is still alive.
    Follower {
        /// Duration where lock is definitely valid.
        valid_for: Duration,
        /// Information provided by the leader
        leader_info: serde_json::Value,
    },
}

#[tracing::instrument(skip(lease), fields(lease_name = lease.metadata.name.as_deref().unwrap_or("<missing>")))]
fn check_lease(lease: &Lease) -> Option<Duration> {
    let spec = match lease.spec.as_ref() {
        Some(s) => s,
        None => {
            tracing::info!("Lease has missing spec, treating as expired");
            return None;
        }
    };
    let last_renew = match &spec.renew_time {
        Some(t) => t.0.clone(),
        None => {
            tracing::info!("Lease has missing renewTime, treating as expired");
            return None;
        }
    };
    let timeout = match spec.lease_duration_seconds {
        Some(t) => t,
        None => {
            tracing::info!("Lease has missing leaseDurationSeconds, treating as expired");
            return None;
        }
    };
    let valid_to = last_renew + chrono::Duration::seconds(timeout.into());
    tracing::info!(lease_valid_to=%valid_to);
    let now = chrono::Utc::now();
    let remaining = valid_to.signed_duration_since(now);
    if remaining.num_milliseconds() <= 0 {
        tracing::info!(
            "Lease expired {} milliseconds ago",
            -remaining.num_milliseconds()
        );
        return None;
    }
    Some(
        remaining
            .to_std()
            .expect("it was checked that duration is non-negative"),
    )
}

/// Tries to acquire a lock. Accepts k8s client, lock params and additional
/// application-specific data identifying this instance (it can be Null if
/// not needed).
#[tracing::instrument(skip(client, params, data))]
pub async fn try_lock(
    client: kube::Client,
    params: LockParams,
    data: serde_json::Value,
) -> anyhow::Result<LockResult> {
    let api = Api::<Lease>::namespaced(client.clone(), &params.namespace);
    loop {
        let lease = match api.get(&params.name).await {
            Ok(lease) => Some(lease),
            Err(err) => {
                if crate::errors::classify_kube(&err) != ErrorClass::NotFound {
                    return Err(err).context("failed to get existing lease");
                }
                None
            }
        };
        let valid_for = lease.as_ref().and_then(|lease| check_lease(lease));
        match valid_for {
            Some(d) => {
                let holder =
                    read_lock_content(lease.as_ref().expect("missing lease is always expired"))
                        .context("missing holderIdentity")?;
                let content: LockContent =
                    serde_json::from_str(&holder).context("failed to parse holderIdentity")?;
                return Ok(LockResult::Follower {
                    valid_for: d,
                    leader_info: content.data,
                });
            }
            None => {
                tracing::info!("Trying to become a leader");
                let new_lease =
                    make_lease(lease.as_ref(), &params.name, &data, params.lock_timeout());
                let res = try_replace_lease(&api, lease.as_ref(), new_lease).await?;
                match res {
                    TryReplaceOutcome::Success(lease) => {
                        tracing::info!("Leader lock acquired successfully");
                        let guard = LockGuard::new(api, &params.name, &params, data, lease);
                        return Ok(LockResult::Leader(guard));
                    }
                    TryReplaceOutcome::Conflict => {
                        tracing::debug!("Conflict, will retry");
                    }
                }
            }
        }
        let base_sleep_time = params
            .conflict_sleep
            .map(|d| d.as_millis())
            .unwrap_or(DEFAULT_CONFLICT_SKIP_MILLIS);
        let dist = rand::distributions::Uniform::new(1.0, 1.1);
        let factor = dist.sample(&mut rand::thread_rng());
        let sleep_time = (base_sleep_time as f64 * factor) as u128;
        let sleep_time = Duration::from_millis(sleep_time.try_into().context("too much sleep")?);
        tokio::time::sleep(sleep_time).await;
    }
}

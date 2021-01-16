//! Utilities for writing dynamic storage privisioner
mod cleanup;
mod provision;

use anyhow::Context as _;
use futures_util::future::BoxFuture;
use k8s_openapi::{
    api::{
        core::v1::{
            Event as K8sEvent, EventSource, ObjectReference, PersistentVolume,
          PersistentVolumeSpec,
        },
        storage::v1::StorageClass,
    },
    apimachinery::pkg::apis::meta::v1::{LabelSelector, MicroTime, Time},
};
use kube::{
    api::{Meta, ObjectMeta},
    Api,
};
use kube_runtime::{
    controller::{Context, ReconcilerAction},
    reflector::Store,
};
use std::{
    collections::BTreeMap,
    fmt::{self, Display, Formatter},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// Something that is able to represent PVC requirements
pub trait FromLabelSelector: Sized {
    /// Should parse provided selector.
    /// If pvc specified selector as None,
    /// in provided value both `match_labels` and `match_expressions`
    /// will be None.
    fn from_selector(selector: LabelSelector) -> anyhow::Result<Self>;
}

/// Use it to disallow non-empty PVC selector
pub struct DenyLabelSelector;
impl FromLabelSelector for DenyLabelSelector {
    fn from_selector(selector: LabelSelector) -> anyhow::Result<Self> {
        if selector.match_expressions.is_none() && selector.match_labels.is_none() {
            Ok(DenyLabelSelector)
        } else {
            anyhow::bail!(".spec.selector must be empty")
        }
    }
}

/// Something that is able to represent SC requirements
pub trait FromParameters: Sized {
    /// Should parse provided params.
    /// If they were missing in the SC definition, Null will be passed.
    fn from_parameters(params: &BTreeMap<String, String>) -> anyhow::Result<Self>;
}

/// Use it to disallow non-empty SC parameters
pub struct DenyParameters;
impl FromParameters for DenyParameters {
    fn from_parameters(params: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        if params.len() == 0 {
            return Ok(Self);
        }
        anyhow::bail!(".parameters must be empty")
    }
}

impl<T: serde::de::DeserializeOwned> FromParameters for T {
    fn from_parameters(params: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let val = serde_json::Value::Object(
            params
                .clone()
                .into_iter()
                .map(|(k, v)| (k, serde_json::Value::String(v)))
                .collect(),
        );
        serde_json::from_value(val).context("Failed to deserialize")
    }
}

/// Event to file against the PVC
#[derive(Debug)]
pub struct Event {
    pub message: String,
    pub ty: EventType,
}

impl Display for Event {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let ty = match self.ty {
            EventType::Normal => "info",
            EventType::Warning => "error",
        };
        format_args!("[{}] {}", ty, self.message).fmt(f)
    }
}

#[derive(Debug)]
pub enum EventType {
    Normal,
    Warning,
}

async fn do_report_error<K: Meta>(k: &kube::Client, ev: &Event, obj: &K) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let event_type = match &ev.ty {
        EventType::Normal => "Normal",
        EventType::Warning => "Warning",
    };
    let ev = K8sEvent {
        metadata: ObjectMeta {
            generate_name: Some(format!("{}-event", obj.name())),
            namespace: obj.namespace(),
            ..Default::default()
        },
        type_: Some(event_type.to_string()),
        source: Some(EventSource {
            host: None,
            component: Some("kube-utils.rs/storage-provisioner".to_string()),
        }),
        count: Some(1),
        reporting_component: Some("kube-utils.rs/storage-provisioner".to_string()),
        // TODO: set out pod name
        reporting_instance: None,
        message: Some(ev.message.to_string()),
        involved_object: make_obj_ref(obj),
        first_timestamp: Some(Time(now.clone())),
        last_timestamp: Some(Time(now.clone())),
        event_time: Some(MicroTime(now)),
        ..Default::default()
    };

    let evs_api = Api::<K8sEvent>::all(k.clone());
    evs_api
        .create(&Default::default(), &ev)
        .await
        .context("failed to post an event")?;

    Ok(())
}

async fn report_error<K: Meta>(k: &kube::Client, ev: &Event, obj: &K) {
    if let Err(e) = do_report_error::<K>(k, ev, obj).await {
        tracing::warn!("Failed to send an event: {:#}", e);
    }
}

fn make_obj_ref<K: Meta>(obj: &K) -> ObjectReference {
    ObjectReference {
        api_version: Some(K::API_VERSION.to_string()),
        kind: Some(K::KIND.to_string()),
        name: Some(obj.name()),
        namespace: obj.namespace(),
        resource_version: obj.resource_ver(),
        uid: obj.meta().uid.clone(),
        field_path: None,
    }
}

/// Possible volume mode
#[derive(Debug, strum::EnumString, strum::Display, Clone, Copy, PartialEq, Eq)]
pub enum VolumeMode {
    /// Volume is a filesystem
    Filesystem,
    /// Volume is a raw block device
    Block,
}

/// Possible volume access modes
#[derive(Debug, strum::EnumString, strum::Display, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    /// Shared and mutable
    ReadWriteMany,
    /// Unique and mutable
    ReadWriteOnce,
    /// Shared and immutable
    ReadOnlyMany,
}

/// Result of successful provisioning
pub struct ProvisionedVolume {
    /// Spec of the PV. There is no need to set `claim_ref` or `access_modes`,
    /// because they will be set automatically,
    pub pv_spec: PersistentVolumeSpec,
    /// Additional labels to set on the PV. Please note that they must match
    /// selector on the PVC.
    pub labels: BTreeMap<String, String>,
    /// Additional annotations to set on the PV.
    pub annotations: BTreeMap<String, String>,
}

/// Building blocks for creating a provisioner
pub trait Provision: Send + Sync + 'static {
    /// Name of the provisioner, e.g. 'example.org/super-volume'
    const NAME: &'static str;
    /// Supported VolumeModes
    const VOLUME_MODES: &'static [VolumeMode];
    /// Supported AccessModes
    const ACCESS_MODES: &'static [AccessMode];

    /// Type representing PVC requirements (.spec.selector).
    type Labels: FromLabelSelector + Send + Sync + 'static;
    /// Type representing StorageClass requirements (.parameters).
    type Parameters: FromParameters + Send + Sync + 'static;
    /// Function that actually provisions a volume.
    /// # Error context
    /// If a context contains `Event`, it will be reported to user.
    /// Otherwise it will be logged.
    fn provision(
        &self,
        labels: Self::Labels,
        params: Self::Parameters,
        volume_mode: VolumeMode,
        access_modes: &[AccessMode],
    ) -> futures_util::future::BoxFuture<'static, anyhow::Result<ProvisionedVolume>>;
    /// Function thet deletes provisioned volume.
    /// It should only release underlying resources. PV will be deleted
    /// automatically.
    /// # Error context
    /// If a context contains `Event`, it will be reported to user.
    /// Otherwise it will be logged.
    fn cleanup(&self, pv: PersistentVolume) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// Controller configuration
#[derive(Debug)]
pub struct Configuration {
    /// Timeout after failed reconcillation
    pub error_timeout: Duration,
    /// Timeout after successful reconcillation
    pub success_timeout: Option<Duration>,
    /// GenerateName prefix for the created PVs
    pub pv_name_prefix: String,
}

impl Default for Configuration {
    fn default() -> Self {
        Configuration {
            error_timeout: Duration::from_secs(10),
            success_timeout: None,
            pv_name_prefix: "pv-".to_string(),
        }
    }
}

struct ContextData<P> {
    kube_client: kube::Client,
    storage_classes: Store<StorageClass>,
    provisioner: P,
    cfg: Configuration,
    cancel: CancellationToken,
}

/// Controller entry point
#[tracing::instrument(skip(provisioner, cancel, cfg, kube_client), fields(provisioner_name = P::NAME))]
pub async fn run<P: Provision>(
    kube_client: &kube::Client,
    provisioner: P,
    cfg: Configuration,
    cancel: CancellationToken,
) {
    tracing::info!(config = ?cfg);
    let cx = Context::new(ContextData {
        kube_client: kube_client.clone(),
        provisioner,
        storage_classes: crate::make_reflector(Api::all(kube_client.clone()), cancel.clone()),
        cfg,
        cancel,
    });

    let provisioner_fut = provision::run(cx.clone());
    let cleanup_fut = cleanup::run(cx);
    let _verify_futures_return_unit: ((), ()) = tokio::join!(provisioner_fut, cleanup_fut);
}

fn error_policy<P>(err: &std::convert::Infallible, _: Context<ContextData<P>>) -> ReconcilerAction {
    match *err {}
}

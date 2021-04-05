use crate::{
    controller::{
        reconcile_queue::{QueueSender, TaskKey},
        DynController,
    },
    multiwatch::{Watcher, WatcherSet},
};
use kube::api::{ApiResource, ResourceExt};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub(crate) struct NonOwnerWaits {
    map: HashMap<(ApiResource, String), HashMap<String, Vec<(String, Option<String>)>>>,
}

impl NonOwnerWaits {
    /// Used by ReconcileContext.
    ///
    /// If child is cluster-scoped, child_ns can be arbitrary.
    pub(crate) fn register_wait(
        &mut self,
        child_resource: ApiResource,
        child_name: String,
        child_ns: Option<String>,
        toplevel_name: String,
        toplevel_ns: Option<String>,
    ) {
        self.map
            .entry((child_resource, child_name))
            .or_default()
            .entry(child_ns.unwrap_or_default())
            .or_default()
            .push((toplevel_name, toplevel_ns))
    }

    pub(crate) fn new() -> Self {
        NonOwnerWaits {
            map: HashMap::new(),
        }
    }

    /// Returns toplevel resources that are interested in this child.
    /// If child_ns is None, child is considered cluster-scoped
    fn extract(
        &mut self,
        res: &ApiResource,
        child_name: &str,
        child_ns: Option<&str>,
    ) -> Vec<(String, Option<String>)> {
        let main_key = (res.clone(), child_name.to_string());
        let by_ns_view = match self.map.get_mut(&main_key) {
            Some(v) => v,
            None => return Vec::new(),
        };
        match child_ns {
            Some(ns) => {
                let res = by_ns_view.remove(ns);
                if res.is_some() && by_ns_view.is_empty() {
                    self.map.remove(&main_key);
                }
                res.unwrap_or_default()
            }
            None => {
                let waiters_iter = std::mem::take(by_ns_view).into_iter().map(|(_, v)| v);
                let mut waiters = Vec::new();
                for mut w in waiters_iter {
                    waiters.append(&mut w);
                }
                self.map.remove(&main_key);
                waiters
            }
        }
    }
}

async fn watch_toplevel(mut watch: Watcher, tx: QueueSender) {
    tracing::info!("Starting watch");
    loop {
        let item = watch.next().await;
        // toplevel resource definitely needs reconcilation
        let key = TaskKey {
            name: item.name(),
            namespace: item.namespace().unwrap_or_default(),
        };
        tx.send(key).await;
    }
}

async fn watch_child(
    mut watch: Watcher,
    tx: QueueSender,
    toplevel_resource: ApiResource,
    child_resource: ApiResource,
    special_waits: Arc<Mutex<NonOwnerWaits>>,
) {
    tracing::info!("Starting watch");
    loop {
        let item = watch.next().await;
        // let's inspect ownerReferences and see if any of them references toplevel resource
        for own_ref in item.owner_references() {
            if own_ref.api_version == toplevel_resource.api_version
                && own_ref.kind == toplevel_resource.kind
            {
                let key = TaskKey {
                    name: own_ref.name.clone(),
                    namespace: item.namespace().unwrap_or_default(),
                };
                tx.send(key).await;
            }
        }
        // maybe some non-owner wants this object
        let waiters = {
            let mut waits = special_waits.lock().await;
            waits.extract(&child_resource, &item.name(), item.namespace().as_deref())
        };
        for w in waiters {
            let key = TaskKey {
                name: w.0,
                namespace: w.1.unwrap_or_default(),
            };
            tx.send(key).await;
        }
    }
}

#[tracing::instrument(skip(
    dc,
    ws,
    cancel,
    toplevel_resource,
    other_resources,
    non_owner_waits,
    tx
))]
pub(crate) async fn detector(
    dc: DynController,
    ws: Arc<WatcherSet>,
    cancel: CancellationToken,
    toplevel_resource: ApiResource,
    other_resources: Vec<ApiResource>,
    non_owner_waits: Arc<Mutex<NonOwnerWaits>>,
    tx: QueueSender,
) {
    let mut resources = Vec::new();
    resources.push((dc.vtable.api_resource)());

    let mut watches = Vec::new();

    let h = tokio::task::spawn(
        watch_toplevel(ws.watch(&toplevel_resource).await, tx.clone())
            .instrument(tracing::info_span!("toplevel_watch")),
    );
    watches.push(h);
    for other in other_resources {
        let span = tracing::info_span!(
            "child_watch",
            api_version = other.api_version.as_str(),
            kind = other.kind.as_str()
        );
        let h = tokio::task::spawn(
            watch_child(
                ws.watch(&other).await,
                tx.clone(),
                toplevel_resource.clone(),
                other,
                non_owner_waits.clone(),
            )
            .instrument(span),
        );
        watches.push(h);
    }
    cancel.cancelled().await;
    tracing::info!("Detector was cancelled, aborting watchers");
    for h in watches {
        h.abort();
    }
}

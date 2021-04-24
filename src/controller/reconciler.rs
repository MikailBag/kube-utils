use crate::{
    applier::{Applier, Strategy},
    controller::{detector::NonOwnerWaits, reconcile_queue::QueueReceiver, DynController},
    multiwatch::WatcherSet,
};
use kube::api::{ApiResource, Resource, ResourceExt};
use kube_runtime::reflector::ObjectRef;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;

/// Context available to reconciler
pub struct ReconcileContext {
    client: kube::Client,
    applier: Applier,
    ws: Arc<WatcherSet>,
    namespace: Option<String>,
    toplevel_name: String,
    non_owner_waits: Arc<Mutex<NonOwnerWaits>>,
}

impl ReconcileContext {
    pub fn client(&self) -> kube::Client {
        self.client.clone()
    }

    pub fn applier(&self) -> Applier {
        self.applier.clone()
    }

    pub async fn cached<K: Resource<DynamicType = ()> + DeserializeOwned>(
        &self,
        name: &str,
    ) -> Option<K> {
        let api_res = ApiResource::erase::<K>(&());
        {
            let mut waits = self.non_owner_waits.lock().await;
            waits.register_wait(
                api_res.clone(),
                name.to_string(),
                self.namespace.clone(),
                self.toplevel_name.clone(),
                self.namespace.clone(),
            )
        }
        let store = self.ws.local_store(&api_res).await?;
        let mut obj_ref = ObjectRef::new_with(name, api_res);
        if let Some(ns) = self.namespace.as_deref() {
            obj_ref = obj_ref.within(ns);
        }
        let obj = store.get(&obj_ref)?;
        let obj = serde_json::to_string(&obj).expect("failed to serialize DynamicObject");
        let obj = serde_json::from_str(&obj).expect("failed to parse resource");
        Some(obj)
    }
}

pub enum ReconcileStatus {
    /// Object is fully reconciled
    Done,
}

/// Gets reconcilation tasks and passes them to the controller.
pub(crate) async fn worker(
    dc: DynController,
    ws: Arc<WatcherSet>,
    rx: QueueReceiver,
    non_owner_waits: Arc<Mutex<NonOwnerWaits>>,
    client: kube::Client,
) {
    let resource = (dc.vtable.api_resource)();
    while let Some(task) = rx.recv().await {
        // fetch latest object from the cache
        let object = {
            let st = ws
                .local_store(&resource)
                .await
                .expect("Store for top-level resource does not exist");
            let obj_ref = ObjectRef::new_with(&task.name, resource.clone()).within(&task.namespace);
            st.get(&obj_ref)
        };
        let object = match object {
            Some(o) => o,
            None => {
                // object no longer exists in the cache.
                // it means it was deleted, so no need to reconcile
                // if it will be created again, we will receive another event
                // for that.
                continue;
            }
        };
        let dc = dc.clone();
        let client = client.clone();
        let ws = ws.clone();
        let non_owner_waits = non_owner_waits.clone();
        let applier = Applier::new(
            client.clone(),
            object.namespace().as_deref(),
            Strategy::Apply {
                field_manager: format!("controller-{}", dc.meta.name),
            },
            crate::applier::Hook::null(),
        );
        async move {
            tracing::debug!("Reconciling {:?}", task);
            let mut cx = ReconcileContext {
                client,
                applier,
                ws,
                namespace: object.metadata.namespace.clone(),
                toplevel_name: object.name(),
                non_owner_waits,
            };
            let fut = (dc.vtable.reconcile)(object, &mut cx);
            match fut.await {
                Ok(_) => {
                    tracing::info!("Reconciled successfully");
                }
                Err(err) => {
                    tracing::warn!("Reconcilation failed: {:#}", err);
                }
            }
        }
        .instrument(tracing::info_span!("Processing reconcilation task"))
        .await;
    }
}

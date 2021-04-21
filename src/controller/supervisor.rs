//! Supervisor manages one controller

use crate::{
    applier::{Applier, Strategy},
    controller::{DynController, ReconcileContext},
    multiwatch::WatcherSet,
};
use core::convert::Infallible;
use futures::lock::Mutex;
use kube::api::{ApiResource, DynamicObject, ResourceExt};
use kube::client::Discovery;
use kube_runtime::reflector::ObjectRef;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

pub(super) struct SupervisorCtl {
    cancel: CancellationToken,
    closed_when_stopped: mpsc::Receiver<Infallible>,
}

impl SupervisorCtl {
    pub(super) fn get_cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(super) async fn wait(mut self) {
        self.closed_when_stopped.recv().await;
    }
}

struct SupervisorState {
    ws: Arc<WatcherSet>,
    apis: Arc<Discovery>,
    client: kube::Client,
    _alive: mpsc::Sender<Infallible>,
}

const NUM_WORKERS: usize = 2;

#[tracing::instrument(skip(dc, ws, apis, client), fields(controller_name = dc.meta.name.as_str()))]
pub(super) fn supervise(
    dc: DynController,
    ws: Arc<WatcherSet>,
    apis: Arc<Discovery>,
    client: kube::Client,
) -> SupervisorCtl {
    tracing::info!(
        controller = dc.meta.name.as_str(),
        workers_count = NUM_WORKERS,
        "Starting supervisor"
    );

    let cancel = CancellationToken::new();
    let (alive_tx, alive_rx) = mpsc::channel(1);

    let state = SupervisorState {
        ws,
        apis,
        client,
        _alive: alive_tx,
    };
    let state = Arc::new(state);

    let (reconcile_tx, reconcile_rx) = mpsc::channel(NUM_WORKERS);
    let reconcile_rx = Arc::new(Mutex::new(reconcile_rx));

    let detector_fut = {
        let fut = detector(
            dc.clone(),
            state.clone(),
            cancel.clone(),
            (dc.vtable.api_resource)(),
            reconcile_tx,
        );
        let cancel = cancel.clone();
        async move {
            tokio::select! {
                _ = fut => {},
                _ = cancel.cancelled() => {
                    tracing::info!("Detector was cancelled")
                }
            }
        }
        .in_current_span()
    };
    tokio::task::spawn(detector_fut);
    for _ in 0..NUM_WORKERS {
        tokio::task::spawn(reconciler(dc.clone(), state.clone(), reconcile_rx.clone()));
    }
    SupervisorCtl {
        cancel,
        closed_when_stopped: alive_rx,
    }
}

#[derive(Debug)]
struct ReconcilationTask {
    object: DynamicObject,
}

async fn reconciler(
    dc: DynController,
    state: Arc<SupervisorState>,
    rx: Arc<Mutex<mpsc::Receiver<ReconcilationTask>>>,
) {
    let recv = move || {
        let rx = rx.clone();
        async move { rx.lock().await.recv().await }
    };
    while let Some(task) = recv().await {
        let dc = dc.clone();
        let client = state.client.clone();
        let ws = state.ws.clone();
        let applier = Applier::new(
            client.clone(),
            ResourceExt::namespace(&task.object).as_deref(),
            Strategy::Apply {
                field_manager: format!("controller-{}", dc.meta.name),
            },
            crate::applier::Hook::null(),
        );
        async move {
            tracing::info!("Reconciling {:?}", task);
            let mut cx = ReconcileContext {
                client,
                applier,
                ws,
                namespace: task.object.metadata.namespace.clone(),
            };
            let fut = (dc.vtable.reconcile)(task.object, &mut cx);
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

#[tracing::instrument(skip(dc, state, cancel, toplevel_resource, reconcile_tx))]
async fn detector(
    dc: DynController,
    state: Arc<SupervisorState>,
    cancel: CancellationToken,
    toplevel_resource: ApiResource,
    reconcile_tx: mpsc::Sender<ReconcilationTask>,
) {
    let mut resources = Vec::new();
    resources.push((dc.vtable.api_resource)());
    let (changed_tx, mut changed_rx) = mpsc::channel(16);
    for res in resources {
        tracing::info!(
            "Starting watch for {} {}",
            res.api_version.as_str(),
            res.kind.as_str()
        );
        let tx = changed_tx.clone();

        let mut watch = state.ws.watch(&res).await;
        let cancel = cancel.clone();
        tokio::task::spawn(
            async move {
                loop {
                    tokio::select! {
                        item = watch.next() => {
                            tracing::debug!("Received event from the watchset");
                            let mut obj_ref = ObjectRef::<DynamicObject>::new_with(&item.name(), res.clone());
                            if let Some(ns) = ResourceExt::namespace(&*item ) {
                                obj_ref = obj_ref.within(&ns);
                            }
                            if let Err(_) = tx.send((obj_ref, res.clone())).await {
                                tracing::debug!("Detector already closed");
                            }
                        }
                        _ = cancel.cancelled() => {
                            break;
                        }
                    }
                }
            }
            .in_current_span(),
        );
    }

    // TODO: fight against cycles
    while let Some(initial_change) = changed_rx.recv().await {
        tracing::info!("Processing applied object {:?}", initial_change.0);
        let mut q = Vec::new();
        q.push(initial_change);
        while let Some((obj_ref, resource)) = q.pop() {
            tracing::debug!("Visiting {:?}", obj_ref);
            let object = state
                .ws
                .local_store(&resource)
                .await
                .and_then(|store| store.get(&obj_ref));
            let object = match object {
                Some(o) => o,
                None => {
                    tracing::debug!("Object is not cached yet");
                    continue;
                }
            };
            let object_typemeta = match object.types.as_ref() {
                Some(tm) => tm,
                None => {
                    tracing::warn!("TypeMeta missing");
                    continue;
                }
            };
            // TODO: can be looked up in hashmap instead
            let (group, version) = match Discovery::parse_api_version(&object_typemeta.api_version)
            {
                Some(parse) => parse,
                None => {
                    tracing::warn!("Failed to parse apiVersion {}", object_typemeta.api_version);
                    continue;
                }
            };
            let api_resource =
                state
                    .apis
                    .resolve_group_version_kind(group, version, &object_typemeta.kind);
            let api_resource = match api_resource {
                Some((ar, _extras)) => ar,
                None => {
                    tracing::warn!(
                        api_version = object_typemeta.api_version.as_str(),
                        kind = object_typemeta.kind.as_str(),
                        "Unknown apiVersion & kind combinarion"
                    );
                    continue;
                }
            };
            if api_resource == toplevel_resource {
                let name = match ResourceExt::namespace(&object) {
                    Some(ns) => format!("{}/{}", ns, object.name()),
                    None => object.name(),
                };
                tracing::info!("Object {} needs reconcilation", name);
                reconcile_tx
                    .send(ReconcilationTask {
                        object: object.clone(),
                    })
                    .await
                    .ok();
            }
            for owner_ref in object.owner_references() {
                let (owner_group, owner_version) =
                    Discovery::parse_api_version(&owner_ref.api_version)
                        .expect("invalid apiVersion in ownerReference");
                let owner_api_resource = state.apis.resolve_group_version_kind(
                    owner_group,
                    owner_version,
                    &owner_ref.kind,
                );
                let owner_api_resource = match owner_api_resource {
                    Some((ar, _)) => ar,
                    None => {
                        tracing::warn!(
                            "Failed to resolve ApiResource for ownerReference {:?}",
                            owner_ref
                        );
                        continue;
                    }
                };
                let obj_ref = ObjectRef::<DynamicObject>::new_with(
                    &owner_ref.name,
                    owner_api_resource.clone(),
                );
                q.push((obj_ref, owner_api_resource));
            }
        }
    }
}

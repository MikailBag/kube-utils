//! Supervisor manages one controller

use crate::{
    applier::{Applier, Strategy},
    controller::{DynController, ReconcileContext},
    multiwatch::{Watcher, WatcherSet},
};
use core::convert::Infallible;
use futures::lock::Mutex;
use kube::api::{ApiResource, DynamicObject, ResourceExt};
use kube_runtime::reflector::{ObjectRef, Store};
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
    //apis: Arc<Discovery>,
    client: kube::Client,
    _alive: mpsc::Sender<Infallible>,
}

const NUM_WORKERS: usize = 2;

#[tracing::instrument(skip(dc, ws, /*apis,*/ client), fields(controller_name = dc.meta.name.as_str()))]
pub(super) fn supervise(
    dc: DynController,
    ws: Arc<WatcherSet>,
    //apis: Arc<Discovery>,
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
        //apis,
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
            Vec::new(),
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
            tracing::debug!("Reconciling {:?}", task);
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

async fn watch_toplevel(mut watch: Watcher, reconcile_tx: mpsc::Sender<ReconcilationTask>) {
    loop {
        let item = watch.next().await;
        // toplevel resource definitely needs reconcilation
        let task = ReconcilationTask {
            object: (*item).clone(),
        };
        let closed = reconcile_tx.send(task).await.is_err();
        if closed {
            tracing::info!("Channel closed");
            break;
        }
    }
}

async fn watch_child(
    mut watch: Watcher,
    reconcile_tx: mpsc::Sender<ReconcilationTask>,
    toplevel_resource: ApiResource,
    toplevel_store: Store<DynamicObject>,
) {
    loop {
        let item = watch.next().await;
        // let's inspect ownerReferences and see if any of them references toplevel resource
        for own_ref in item.owner_references() {
            if own_ref.api_version == toplevel_resource.api_version
                && own_ref.kind == toplevel_resource.kind
            {
                let mut obj_ref =
                    ObjectRef::<DynamicObject>::new_with(&own_ref.name, toplevel_resource.clone());
                if let Some(ns) = ResourceExt::namespace(&*item) {
                    obj_ref = obj_ref.within(&ns);
                }
                let object = match toplevel_store.get(&obj_ref) {
                    Some(o) => o,
                    None => {
                        tracing::debug!(dangling = ?own_ref, "Referenced toplevel resource does not exist in cache yet");
                        continue;
                    }
                };
                let task = ReconcilationTask { object };
                let closed = reconcile_tx.send(task).await.is_err();
                if closed {
                    tracing::info!("Channel closed");
                    return;
                }
            }
        }
    }
}

#[tracing::instrument(skip(dc, state, cancel, toplevel_resource, reconcile_tx))]
async fn detector(
    dc: DynController,
    state: Arc<SupervisorState>,
    cancel: CancellationToken,
    toplevel_resource: ApiResource,
    other_resources: Vec<ApiResource>,
    reconcile_tx: mpsc::Sender<ReconcilationTask>,
) {
    let mut resources = Vec::new();
    resources.push((dc.vtable.api_resource)());

    let mut watches = Vec::new();

    let h = tokio::task::spawn(
        watch_toplevel(
            state.ws.watch(&toplevel_resource).await,
            reconcile_tx.clone(),
        )
        .instrument(tracing::info_span!("Toplevel resource watcher")),
    );
    watches.push(h);
    for other in other_resources {
        let span = tracing::info_span!(
            "Child resource watcher",
            api_version = other.api_version.as_str(),
            kind = other.kind.as_str()
        );
        let h = tokio::task::spawn(
            watch_child(
                state.ws.watch(&other).await,
                reconcile_tx.clone(),
                toplevel_resource.clone(),
                state
                    .ws
                    .local_store(&toplevel_resource)
                    .await
                    .expect("store for toplevel resource was already created earlier"),
            )
            .instrument(span),
        );
        watches.push(h);
    }
    tokio::task::spawn(async move {
        cancel.cancelled().await;
        tracing::info!("Detector was cancelled, aborting watchers");
        for h in watches {
            h.abort();
        }
    });
}

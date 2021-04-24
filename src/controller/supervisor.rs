//! Supervisor manages one controller

use crate::{
    applier::{Applier, Strategy},
    controller::{
        detector::{detector, NonOwnerWaits},
        reconcile_queue::{QueueConfig, QueueReceiver},
        DynController, ReconcileContext,
    },
    multiwatch::WatcherSet,
};
use core::convert::Infallible;
use kube::api::ResourceExt;
use kube_runtime::reflector::ObjectRef;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
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

#[tracing::instrument(skip(dc, ws, client, cfg), fields(controller_name = dc.meta.name.as_str()))]
pub(super) fn supervise(
    dc: DynController,
    ws: Arc<WatcherSet>,
    client: kube::Client,
    cfg: QueueConfig,
) -> SupervisorCtl {
    tracing::info!(
        controller = dc.meta.name.as_str(),
        workers_count = NUM_WORKERS,
        queue_config = ?cfg,
        "Starting supervisor"
    );

    let cancel = CancellationToken::new();
    let (alive_tx, alive_rx) = mpsc::channel(1);

    let state = SupervisorState {
        ws,
        client,
        _alive: alive_tx,
    };
    let state = Arc::new(state);

    let (tx, rx) = crate::controller::reconcile_queue::queue(cfg);

    let non_owner_waits = Arc::new(Mutex::new(NonOwnerWaits::new()));

    let detector_fut = {
        let fut = detector(
            dc.clone(),
            state.ws.clone(),
            cancel.clone(),
            (dc.vtable.api_resource)(),
            dc.meta.watches.clone(),
            non_owner_waits.clone(),
            tx,
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
        tokio::task::spawn(reconciler(
            dc.clone(),
            state.clone(),
            rx.clone(),
            non_owner_waits.clone(),
        ));
    }
    SupervisorCtl {
        cancel,
        closed_when_stopped: alive_rx,
    }
}

async fn reconciler(
    dc: DynController,
    state: Arc<SupervisorState>,
    rx: QueueReceiver,
    non_owner_waits: Arc<Mutex<NonOwnerWaits>>,
) {
    let resource = (dc.vtable.api_resource)();
    while let Some(task) = rx.recv().await {
        // fetch latest object from the cache
        let object = {
            let st = state
                .ws
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
        let client = state.client.clone();
        let ws = state.ws.clone();
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

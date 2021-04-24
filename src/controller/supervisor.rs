//! Supervisor manages one controller

use crate::{
    controller::{
        detector::{detector, NonOwnerWaits},
        reconcile_queue::{QueueConfig},
        reconciler::worker,
        DynController,
    },
    multiwatch::WatcherSet,
};
use core::convert::Infallible;
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

    let (tx, rx) = crate::controller::reconcile_queue::queue(cfg);

    let non_owner_waits = Arc::new(Mutex::new(NonOwnerWaits::new()));

    let detector_fut = {
        let fut = detector(
            dc.clone(),
            ws.clone(),
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
    let mut tasks = Vec::new();
    tasks.push(tokio::task::spawn(detector_fut));
    for _ in 0..NUM_WORKERS {
        let h = tokio::task::spawn(worker(
            dc.clone(),
           ws.clone(),
            rx.clone(),
            non_owner_waits.clone(),
            client.clone(),
        ));
        tasks.push(h);
    }
    tokio::task::spawn(async move {
        for h in tasks {
            h.await.unwrap();
        }
        drop(alive_tx);
    });
    SupervisorCtl {
        cancel,
        closed_when_stopped: alive_rx,
    }
}

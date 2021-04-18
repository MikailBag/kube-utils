use crate::storage::{error_policy, report_error, ContextData, Event, EventType, Provision};
use futures::stream::StreamExt;
use k8s_openapi::api::core::v1::PersistentVolume;
use kube::{
    api::{ListParams, ResourceExt},
    Api,
};
use kube_runtime::controller::{Context, Controller, ReconcilerAction};

/// Provisioner entry point
#[tracing::instrument(skip(cx))]
pub(super) async fn run<P: Provision>(cx: Context<ContextData<P>>) {
    let pv_lp = ListParams {
        label_selector: Some(format!(
            "storage.kube-utils.rs/provisioner={}",
            base64::encode(P::NAME)
        )),
        ..Default::default()
    };
    let fut = Controller::new(
        Api::<PersistentVolume>::all(cx.get_ref().kube_client.clone()),
        pv_lp,
    )
    .run(reconciler, error_policy, cx.clone())
    .for_each(|item| async {
        if let Err(e) = item {
            tracing::warn!("Controller error: {:#}", e);
        }
    });
    tokio::select! {
        _ = fut => (),
        _ = cx.get_ref().cancel.cancelled() => ()
    }
}

#[tracing::instrument(skip(pv, cx), fields(pv = pv.name().as_str()))]
async fn reconciler<P: Provision>(
    pv: PersistentVolume,
    cx: Context<ContextData<P>>,
) -> Result<ReconcilerAction, std::convert::Infallible> {
    if let Err(message) = check_preconditions(&pv) {
        tracing::info!("Skipping PVC: {}", message);
        return Ok(ReconcilerAction {
            requeue_after: cx.get_ref().cfg.success_timeout,
        });
    };

    tracing::info!("Executing cleanup");
    let cleanup_res = cx.get_ref().provisioner.cleanup(pv.clone()).await;

    if let Err(err) = cleanup_res {
        match err.downcast_ref::<Event>() {
            Some(ev) => {
                report_error(&cx.get_ref().kube_client, ev, &pv).await;
            }
            None => {
                let error_id = uuid::Uuid::new_v4();
                let ev = Event {
                    ty: EventType::Warning,
                    message: format!("Cleanup failed (error id {})", error_id),
                };
                report_error(&cx.get_ref().kube_client, &ev, &pv).await;
                tracing::error!(error_id = %error_id.to_hyphenated(), "Provisioner failed: {:#}", err);
            }
        }
        return Ok(ReconcilerAction {
            requeue_after: Some(cx.get_ref().cfg.error_timeout),
        });
    };
    tracing::info!("Volume was cleaned up successfully, deleting from cluster");
    let pv_api = Api::<PersistentVolume>::all(cx.get_ref().kube_client.clone());
    for attempt_id in 0..5 {
        match pv_api.delete(&pv.name(), &Default::default()).await {
            Ok(_) => {
                tracing::info!(
                    attempt_id = attempt_id,
                    "Successfully triggered PV {} deletion",
                    pv.name()
                );
            }
            Err(e) => {
                tracing::warn!(attempt_id = attempt_id, "Failed to delete a PV: {:#}", e);
            }
        }
    }
    tracing::error!("Failed to push PV");
    Ok(ReconcilerAction {
        requeue_after: Some(cx.get_ref().cfg.error_timeout),
    })
}

fn check_preconditions(pv: &PersistentVolume) -> Result<(), String> {
    let phase = pv
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref());
    if phase != Some("Released") {
        return Err(format!(".status.phase is not Released: {:?}", phase));
    }
    let reclaim_policy = pv
        .spec
        .as_ref()
        .and_then(|spec| spec.persistent_volume_reclaim_policy.as_deref());
    if reclaim_policy != Some("Delete") {
        return Err(format!(
            ".spec.persistentVolumeReclaimPolicy is not Delete: {:?}",
            reclaim_policy
        ));
    }
    Ok(())
}

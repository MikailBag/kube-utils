use crate::storage::{
    error_policy, make_obj_ref, report_error, AccessMode, ContextData, Event, EventType,
    FromLabelSelector, FromParameters, Provision, ProvisionedVolume, VolumeMode,
};
use futures_util::StreamExt;
use k8s_openapi::{
    api::{
        core::v1::{PersistentVolume, PersistentVolumeClaim},
        storage::v1::StorageClass,
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use kube::{
    api::{Meta, ObjectMeta},
    Api,
};
use kube_runtime::{
    controller::{Context, Controller, ReconcilerAction},
    reflector::ObjectRef,
};
use std::collections::BTreeMap;

/// Provisioner entry point
#[tracing::instrument(skip(cx))]
pub(super) async fn run<P: Provision>(cx: Context<ContextData<P>>) {
    let fut = Controller::new(
        Api::<PersistentVolumeClaim>::all(cx.get_ref().kube_client.clone()),
        Default::default(),
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

#[tracing::instrument(skip(pvc, cx), fields(pvc = pvc.name().as_str()))]
async fn reconciler<P: Provision>(
    pvc: PersistentVolumeClaim,
    cx: Context<ContextData<P>>,
) -> Result<ReconcilerAction, std::convert::Infallible> {
    let storage_class = match load_storage_class(&pvc, &cx) {
        Ok(sc) => sc,
        Err(message) => {
            tracing::info!("Skipping PVC: {}", message);
            return Ok(ReconcilerAction {
                requeue_after: cx.get_ref().cfg.success_timeout,
            });
        }
    };

    let labels = match pvc.spec.as_ref().and_then(|spec| spec.selector.as_ref()) {
        Some(s) => s.clone(),
        None => LabelSelector {
            match_expressions: None,
            match_labels: None,
        },
    };

    let labels = match <P::Labels as FromLabelSelector>::from_selector(labels) {
        Ok(s) => Some(s),
        Err(err) => {
            let ev = Event {
                ty: EventType::Warning,
                message: format!("Invalid .spec.selector: {:#}", err),
            };
            report_error(&cx.get_ref().kube_client, &ev, &pvc).await;
            None
        }
    };
    let params = match storage_class.parameters.as_ref() {
        Some(p) => p.clone(),
        None => BTreeMap::new(),
    };
    let params = match <P::Parameters as FromParameters>::from_parameters(&params) {
        Ok(p) => Some(p),
        Err(err) => {
            let ev = Event {
                ty: EventType::Warning,
                message: format!("Invalid .parameters: {:#}", err),
            };
            report_error(&cx.get_ref().kube_client, &ev, &pvc).await;
            None
        }
    };

    let mode_and_access = match parse_modes::<P>(&pvc) {
        Ok(m_and_a) => Some(m_and_a),
        Err(err) => {
            let ev = Event {
                ty: EventType::Warning,
                message: format!(
                    "Invalid .spec.accessModes and/or .spec.volumeMode: {:#}",
                    err
                ),
            };
            report_error(&cx.get_ref().kube_client, &ev, &pvc).await;
            None
        }
    };

    // TODO
    let _volume_size = pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.resources.as_ref())
        .and_then(|r_reqs| r_reqs.requests.as_ref())
        .and_then(|requests| requests.get("storage"))
        .map(|quantity| quantity.0.as_str());

    let (labels, params, (volume_mode, access_modes)) = match (labels, params, mode_and_access) {
        (Some(l), Some(p), Some(m)) => (l, p, m),
        _ => {
            tracing::info!("Aborting reconcillation: PVC or SC was invalid");
            return Ok(ReconcilerAction {
                requeue_after: Some(cx.get_ref().cfg.error_timeout),
            });
        }
    };

    tracing::info!("Executing provisioner");
    let provision: ProvisionedVolume = match cx
        .get_ref()
        .provisioner
        .provision(labels, params, volume_mode, &access_modes)
        .await
    {
        Ok(pv) => pv,
        Err(err) => {
            match err.downcast_ref::<Event>() {
                Some(ev) => {
                    report_error(&cx.get_ref().kube_client, ev, &pvc).await;
                }
                None => {
                    let error_id = uuid::Uuid::new_v4();
                    let ev = Event {
                        ty: EventType::Warning,
                        message: format!("Provisioning failed (error id {})", error_id),
                    };
                    report_error(&cx.get_ref().kube_client, &ev, &pvc).await;
                    tracing::error!(error_id = %error_id.to_hyphenated(), "Provisioner failed: {:#}", err);
                }
            }
            return Ok(ReconcilerAction {
                requeue_after: Some(cx.get_ref().cfg.error_timeout),
            });
        }
    };
    let mut pv = provision.pv_spec.clone();
    pv.access_modes = Some(access_modes.into_iter().map(|x| x.to_string()).collect());
    pv.claim_ref = Some(make_obj_ref(&pvc));
    pv.volume_mode = Some(volume_mode.to_string());

    let mut pv_annotaions = provision.annotations.clone();
    pv_annotaions.insert(
        "pv.kubernetes.io/provisioned-by".to_string(),
        P::NAME.to_string(),
    );
    pv_annotaions.insert(
        "pv.kubernetes.io/storage-class".to_string(),
        storage_class.name(),
    );
    let mut pv_labels = provision.labels.clone();
    pv_labels.insert(
        format!("storage.kube-utils.rs/provisioner"),
        base64::encode(P::NAME),
    );

    let pv = PersistentVolume {
        metadata: ObjectMeta {
            generate_name: Some(cx.get_ref().cfg.pv_name_prefix.clone()),
            labels: Some(pv_labels),
            annotations: Some(pv_annotaions),
            ..Default::default()
        },
        spec: Some(pv),
        status: None,
    };
    tracing::info!("Volume provisioned successfully, pushing to cluster");
    let pv_api = Api::<PersistentVolume>::all(cx.get_ref().kube_client.clone());
    for attempt_id in 0..5 {
        match pv_api.create(&Default::default(), &pv).await {
            Ok(created_pv) => {
                tracing::info!(
                    attempt_id = attempt_id,
                    "Successfully created PV {}",
                    created_pv.name()
                );
            }
            Err(e) => {
                tracing::warn!(attempt_id = attempt_id, "Failed to create a PV: {:#}", e);
            }
        }
    }
    tracing::error!("Failed to push PV");
    Ok(ReconcilerAction {
        requeue_after: Some(cx.get_ref().cfg.error_timeout),
    })
}

fn parse_modes<P: Provision>(
    pvc: &PersistentVolumeClaim,
) -> Result<(VolumeMode, Vec<AccessMode>), String> {
    let volume_mode = match pvc.spec.as_ref().and_then(|spec| spec.volume_mode.as_ref()) {
        Some(vm) => vm
            .parse()
            .map_err(|err| format!("invalid volumeMode: {}", err))?,
        None => VolumeMode::Filesystem,
    };
    if !P::VOLUME_MODES.contains(&volume_mode) {
        return Err(format!("Unsupported volumeMode: {}", volume_mode));
    };
    let access_modes = pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.access_modes.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.parse())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("Invalid accessMode: {}", err))?;
    for mode in &access_modes {
        if !P::ACCESS_MODES.contains(&mode) {
            return Err(format!("Unsupported accessMode: {}", mode));
        }
    }
    Ok((volume_mode, access_modes))
}

fn load_storage_class<P: Provision>(
    pvc: &PersistentVolumeClaim,
    cx: &Context<ContextData<P>>,
) -> Result<StorageClass, String> {
    const ANNOTAION_NAME: &str = "volume.beta.kubernetes.io/storage-provisioner";

    if pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.volume_name.as_ref())
        .is_some()
    {
        return Err("PVC is already bound to a volume".to_string());
    }

    let requested_provisioner = pvc
        .metadata
        .annotations
        .as_ref()
        .and_then(|anns| anns.get(ANNOTAION_NAME))
        .ok_or_else(|| format!("annotaion {} missing", ANNOTAION_NAME))?;

    if requested_provisioner != P::NAME {
        return Err(format!(
            "PVC requests other provisioner ({})",
            requested_provisioner
        ));
    }

    let storage_class_name = pvc
        .spec
        .as_ref()
        .and_then(|spec| spec.storage_class_name.as_ref())
        .ok_or_else(|| ".spec.storageClassName missing".to_string())?;

    let storage_class = cx
        .get_ref()
        .storage_classes
        .get(&ObjectRef::new(storage_class_name))
        .ok_or_else(|| format!("unknown StorageClass {}", storage_class_name))?;

    Ok(storage_class)
}

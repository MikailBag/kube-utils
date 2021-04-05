use crate::{
    applier::{Applier, Strategy},
};
use anyhow::Context as _;
use k8s_openapi::api::core::v1::{
    Container, HostPathVolumeSource, Pod, PodSpec, Volume, VolumeMount,
};
use kube::api::{Api, ObjectMeta, Resource};
use std::{collections::BTreeMap, time::Duration};

pub enum ExecTarget {
    /// A control plane node
    Master,
    /// Node with the specific name
    Named(String),
}

pub struct Mount {
    /// Host path
    pub host: String,
    /// Pod path
    pub pod: String,
}

pub struct ExecParams {
    /// Command to execute, e.g. `[sudo, apt-get, update]`
    pub command: Vec<String>,
    /// Namespace that will contain the pod
    pub namespace: String,
    /// .generateName for pod
    pub pod_name_prefix: String,
    /// Image to use, e.g. `alpine`. Must have
    /// `tail` command available.
    pub image: String,
    /// Target node
    pub target: ExecTarget,
    /// Start timeout.
    pub timeout: Duration,
    /// Extra mounts
    pub mounts: Vec<Mount>,
}

/// Runs command in new pod.
pub async fn exec(
    k: &kube::Client,
    params: ExecParams,
) -> anyhow::Result<(Pod, kube::api::AttachedProcess)> {
    let applier = Applier::new(k.clone(), &params.namespace, Strategy::Create);

    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();
    for (i, mount) in params.mounts.iter().enumerate() {
        let volume_name = format!("mounted-path-{}", i);
        volumes.push(Volume {
            host_path: Some(HostPathVolumeSource {
                path: mount.host.clone(),
                ..Default::default()
            }),
            name: volume_name.clone(),
            ..Default::default()
        });

        volume_mounts.push(VolumeMount {
            mount_path: mount.pod.clone(),
            name: volume_name.clone(),
            ..Default::default()
        });
    }

    let mut pod_spec = PodSpec {
        containers: vec![Container {
            name: "main".to_string(),
            image: Some(params.image.to_string()),
            command: Some(vec![
                "tail".to_string(),
                "-f".to_string(),
                "/dev/null".to_string(),
            ]),
            volume_mounts: Some(volume_mounts),
            ..Default::default()
        }],
        volumes: Some(volumes),
        restart_policy: Some("Never".to_string()),
        ..Default::default()
    };

    match &params.target {
        ExecTarget::Master => {
            let mut selector = BTreeMap::new();
            selector.insert(
                "node-role.kubernetes.io/control-plane".to_string(),
                "".to_string(),
            );
            pod_spec.node_selector = Some(selector);
        }
        ExecTarget::Named(name) => {
            pod_spec.node_name = Some(name.clone());
        }
    }

    let pod = Pod {
        metadata: ObjectMeta {
            generate_name: Some(params.pod_name_prefix.clone() + "-"),
            ..Default::default()
        },
        spec: Some(pod_spec),
        ..Default::default()
    };
    let pod = applier.apply(pod).await.context("failed to create pod")?;

    crate::wait::wait(
        k,
        &mut crate::wait::simple_callback_fn(pod_is_started),
        Some(params.namespace.as_str()),
        &pod.name(),
        params.timeout,
    )
    .await
    .context("failed to wait for pod startup")?;

    let pods_api = Api::<Pod>::namespaced(k.clone(), &params.namespace);

    let process = pods_api
        .exec(&pod.name(), params.command.iter(), &Default::default())
        .await
        .context("failed to start process")?;

    Ok((pod, process))
}

fn pod_is_started(pod: &Pod) -> Option<()> {
    pod.status
        .as_ref()
        .and_then(|status| status.container_statuses.as_ref())
        .and_then(|statuses| statuses.get(0))
        .and_then(|status| status.state.as_ref())
        .and_then(|state| state.running.as_ref())
        .map(drop)
}

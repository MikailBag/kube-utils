use std::str::FromStr;

use anyhow::Context as _;
use async_trait::async_trait;
use k8s_openapi::{
    api::{
        core::v1::{ConfigMap, Pod},
        rbac::v1::ClusterRole,
    },
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::api::{Api, ObjectMeta, Resource, ResourceExt};
use kube::CustomResource;
use kube_utils::{
    builder::Builder,
    controller::{Collect, Controller, ControllerManager, ReconcileContext, ReconcileStatus},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut controller_manager = ControllerManager::new();
    controller_manager.add::<ExecController>();
    controller_manager.main().await
}

#[derive(CustomResource, Debug, Serialize, Deserialize, Clone, JsonSchema, Default)]
#[kube(group = "example.io", version = "v1", kind = "Exec")]
#[kube(status = "ExecStatus")]
struct ExecSpec {
    /// Code to execute. Read-only.
    #[serde(skip_serializing_if = "String::is_empty")]
    code: String,
    /// Language. Read-only.
    #[serde(skip_serializing_if = "String::is_empty")]
    language: String,
}
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, Default)]
struct ExecStatus {
    /// Output of the code
    output: Option<String>,
    /// Name of the pod used to execute code.
    pod_name: Option<String>,
    /// Name of the configmap containing the code.
    configmap_name: Option<String>,
}

fn make_owner_ref<K: Resource>(
    resource: &K,
    dt: &K::DynamicType,
    is_controller: bool,
) -> OwnerReference {
    OwnerReference {
        api_version: K::api_version(dt).to_string(),
        block_owner_deletion: None,
        controller: Some(is_controller),
        kind: K::kind(dt).to_string(),
        name: resource.name(),
        uid: resource.uid().expect("missing uid on persisted object"),
    }
}

enum Language {
    Python3,
    JavaScript,
}

impl FromStr for Language {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lang = match s {
            "python3" => Language::Python3,
            "javascript" => Language::JavaScript,
            other => anyhow::bail!("unknown language '{}'", other),
        };

        Ok(lang)
    }
}

struct ExecController;

#[async_trait]
impl Controller for ExecController {
    fn resource_dynamic_type() -> <Self::Resource as Resource>::DynamicType {
        ()
    }

    type Resource = Exec;

    fn describe<C: Collect>(collector: &C) {
        let mut role = Builder::<ClusterRole>::new();
        role.inner()
            .allow::<Exec>(&["patch"])
            .allow::<Pod>(&["create"]);

        collector
            .name("Exec")
            .crd(Exec::crd())
            .role(role.build().rules.unwrap_or_default());
    }

    async fn reconcile(cx: &mut ReconcileContext, exec: Exec) -> anyhow::Result<ReconcileStatus> {
        let pods =
            Api::<Pod>::namespaced(cx.client().clone(), exec.namespace().as_deref().unwrap());

        let mut status = exec.status.clone().unwrap_or_default();
        if status.output.is_some() {
            tracing::info!("Exec already finished");
            return Ok(ReconcileStatus::Done);
        }
        // TODO report error to user
        let lang: Language = exec
            .spec
            .language
            .parse()
            .context("failed to parse .spec.language")?;
        if status.configmap_name.is_none() {
            tracing::info!("Creating configmap");
            let mut cmap = Builder::<ConfigMap>::new();
            cmap.name_prefix(&exec.name())
                .owner(make_owner_ref(&exec, &(), true));
            cmap.inner().immutable();

            let configmaps = Api::<ConfigMap>::namespaced(
                cx.client().clone(),
                exec.namespace().as_deref().unwrap(),
            );
            let cmap = configmaps
                .create(&Default::default(), &cmap.build())
                .await
                .context("failed to create configmap")?;
            tracing::debug!("Binding configmap {}", cmap.name());
            status.configmap_name = Some(cmap.name());
            cx.applier()
                .apply(Exec {
                    api_version: exec.api_version.clone(),
                    kind: exec.kind.clone(),
                    metadata: ObjectMeta {
                        name: Some(exec.name()),
                        ..Default::default()
                    },
                    spec: Default::default(),
                    status: Some(status.clone()),
                })
                .await?;
        }
        if status.pod_name.is_none() {
            tracing::info!("Creating pod");
            let image = match lang {
                Language::Python3 => "python:3",
                Language::JavaScript => "node:14",
            };

            let mut pod_builder = Builder::<Pod>::new();
            pod_builder
                .name_prefix(&exec.name())
                .owner(make_owner_ref(&exec, &(), true))
                .inner()
                .image(image);

            let pod = pod_builder.build();
            let pod = pods.create(&Default::default(), &pod).await?;
            tracing::debug!("Binding pod {}", pod.name());
            status.pod_name = Some(pod.name());
            cx.applier()
                .apply(Exec {
                    api_version: exec.api_version.clone(),
                    kind: exec.kind.clone(),
                    metadata: ObjectMeta {
                        name: Some(exec.name()),
                        ..Default::default()
                    },
                    spec: Default::default(),
                    status: Some(status.clone()),
                })
                .await?;
        }
        if let Some(pod_name) = status.pod_name.as_ref() {
            tracing::debug!("Checking if pod is completed");
            let pod = cx.cached::<Pod>(pod_name).await;
            if let Some(pod) = pod {
                let completed = pod
                    .status
                    .as_ref()
                    .and_then(|status| status.phase.as_deref())
                    .map_or(false, |phase| phase == "Succeeded" || phase == "Failed");
                if completed {
                    tracing::info!("Pod has completed");
                    let logs = pods.logs(&pod.name(), &Default::default()).await?;
                    status.output = Some(logs);
                }
            }
        }

        Ok(ReconcileStatus::Done)
    }
}

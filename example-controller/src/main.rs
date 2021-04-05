use std::str::FromStr;

use anyhow::Context as _;
use async_trait::async_trait;
use k8s_openapi::api::{
    core::v1::{ConfigMap, Pod},
    rbac::v1::ClusterRole,
};
use kube::api::{Api, ApiResource, ObjectMeta, ResourceExt};
use kube::CustomResource;
use kube_utils::{
    builder::Builder,
    controller::{
        make_owner_reference, Collect, Controller, ControllerManager, ReconcileContext,
        ReconcileStatus,
    },
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
#[kube(namespaced)]
struct ExecSpec {
    /// Code to execute. Read-only.
    #[serde(skip_serializing_if = "String::is_empty")]
    code: String,
    /// Language. Read-only.
    #[serde(skip_serializing_if = "String::is_empty")]
    language: String,
}
#[derive(Debug, Serialize, Deserialize, Clone, JsonSchema, Default, PartialEq, Eq)]
struct ExecStatus {
    /// Output of the code
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<String>,
    /// True if execution has completed
    done: bool,
}

#[derive(Clone, Copy)]
enum Language {
    Python3,
    JavaScript,
}

impl Language {
    fn cmd(self) -> Vec<&'static str> {
        match self {
            Language::JavaScript => vec!["node"],
            Language::Python3 => vec!["python3"],
        }
    }
    fn filename(self) -> &'static str {
        match self {
            Language::JavaScript => "code.js",
            Language::Python3 => "code.py",
        }
    }
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

#[derive(Debug)]
struct Observations {
    configmap_exists: bool,
    execution_started: bool,
    execution_finished: bool,
}

struct ExecController;

fn is_pod_completed(p: &Pod) -> bool {
    p.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .map_or(false, |phase| phase == "Succeeded" || phase == "Failed")
}

impl ExecController {
    async fn observe(cx: &mut ReconcileContext, exec: &Exec) -> Observations {
        let configmap_exists = cx.cached::<ConfigMap>(&exec.name()).await.is_some();
        let (execution_started, execution_finished) = match cx.cached::<Pod>(&exec.name()).await {
            Some(p) => (true, is_pod_completed(&p)),
            None => (false, false),
        };
        Observations {
            configmap_exists,
            execution_started,
            execution_finished,
        }
    }
}

#[async_trait]
impl Controller for ExecController {
    fn resource_dynamic_type() {}

    type Resource = Exec;

    fn describe<C: Collect>(collector: &C) {
        let mut role = Builder::<ClusterRole>::new();
        role.inner()
            .allow::<Exec>(&["patch"])
            .allow::<Pod>(&["create"]);

        collector
            .name("Exec")
            .crd(Exec::crd())
            .role(role.build().rules.unwrap_or_default())
            .watch(ApiResource::erase::<ConfigMap>(&()))
            .watch(ApiResource::erase::<Pod>(&()));
    }

    async fn reconcile(cx: &mut ReconcileContext, exec: Exec) -> anyhow::Result<ReconcileStatus> {
        let pods =
            Api::<Pod>::namespaced(cx.client().clone(), exec.namespace().as_deref().unwrap());

        let observations = Self::observe(cx, &exec).await;
        tracing::info!(observations = ?observations);

        // TODO report error to user
        let lang: Language = exec
            .spec
            .language
            .parse()
            .context("failed to parse .spec.language")?;

        if !observations.configmap_exists {
            tracing::info!("Creating configmap");
            let mut cmap = Builder::<ConfigMap>::new();
            cmap.name(&exec.name())
                .owner(make_owner_reference(&exec, &()));
            cmap.inner()
                .immutable()
                .add(lang.filename(), exec.spec.code.clone().into_bytes());

            let configmaps = Api::<ConfigMap>::namespaced(
                cx.client().clone(),
                exec.namespace().as_deref().unwrap(),
            );
            configmaps
                .create(&Default::default(), &cmap.build())
                .await
                .context("failed to create configmap")?;
        }
        if !observations.execution_started {
            tracing::info!("Creating pod");
            let image = match lang {
                Language::Python3 => "python:3",
                Language::JavaScript => "node:14",
            };
            let mut cmd = lang.cmd();
            let full_file_name = format!("/code/{}", lang.filename());
            cmd.push(&full_file_name);

            let mut pod_builder = Builder::<Pod>::new();
            pod_builder
                .name(&exec.name())
                .owner(make_owner_reference(&exec, &()))
                .inner()
                .image(image)
                .restart_never()
                .args(&cmd)
                .mount_configmap("/code", &exec.name(), "code");

            let pod = pod_builder.build();
            pods.create(&Default::default(), &pod).await?;
        }
        if observations.execution_finished {
            tracing::info!("capturing output");
            let logs = pods.logs(&exec.name(), &Default::default()).await?;
            let status = ExecStatus {
                output: Some(logs),
                done: true,
            };
            if Some(&status) != exec.status.as_ref() {
                tracing::debug!(previous = ?exec.status.as_ref(), new = ?status, "updating .status");
                let updated = cx
                    .update_status(&Exec {
                        api_version: exec.api_version.clone(),
                        kind: exec.kind.clone(),
                        metadata: ObjectMeta {
                            name: Some(exec.name()),
                            namespace: exec.namespace(),
                            ..Default::default()
                        },
                        spec: Default::default(),
                        status: Some(status.clone()),
                    })
                    .await?;
                tracing::trace!(object = ?updated, "new object");
            }
        }
        Ok(ReconcileStatus::Done)
    }
}

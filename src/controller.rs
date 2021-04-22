mod cli;
mod collect;
mod init;
mod supervisor;
mod validate_api_server;

use crate::{applier::Applier, multiwatch::WatcherSet};

pub use self::collect::Collect;
use self::collect::ControllerDescriptionCollector;
use anyhow::Context as _;
use async_trait::async_trait;
use futures::future::FutureExt;
use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::api::{Api, ApiResource, DynamicObject, Resource, ResourceExt};
use kube_runtime::reflector::ObjectRef;
use serde::de::DeserializeOwned;
use std::{sync::Arc, time::Duration};

/// Type, wrapping several controllers and providing all
/// required infrastructure for them to work.
pub struct ControllerManager {
    controllers: Vec<DynController>,
}

impl ControllerManager {
    pub fn new() -> Self {
        ControllerManager {
            controllers: Vec::new(),
        }
    }

    /// Adds a controller
    /// # Panics
    /// Panics if `<C as Controller>::describe` is incorrect
    pub fn add<C: Controller>(&mut self) {
        let collector = ControllerDescriptionCollector::new();
        C::describe(&collector);
        let meta = collector.finalize();
        let vtable = ControllerVtable::of::<C>();
        let dc = DynController { meta, vtable };
        dc.validate();
        self.controllers.push(dc);
    }

    /// Controller manger entry point.
    ///
    /// This function parses command line arguments,
    /// launches web server and serves to completion
    #[tracing::instrument(skip(self))]
    pub async fn main(self) -> anyhow::Result<()> {
        let args: cli::Args = clap::Clap::parse();
        tracing::info!(args = ?args, "parsed command-line arguments");
        match args {
            cli::Args::List => self.print_list(),
            cli::Args::Run(args) => self.run(args).await?,
            cli::Args::PrintCustomResources => self.crd_print(),
            cli::Args::ApplyCustomResources => self.crd_apply().await?,
        }
        Ok(())
    }

    fn print_list(self) {
        println!("Supported controllers:");
        for c in &self.controllers {
            let crd_info = if let Some(crd) = c.meta.crd.as_ref() {
                format!(
                    "(Custom Resource: {}/{})",
                    crd.name(),
                    (c.vtable.api_resource)().version
                )
            } else {
                "".to_string()
            };
            println!("\t{}{}", c.meta.name, crd_info);
        }
    }

    fn crd_print(self) {
        for c in &self.controllers {
            if let Some(crd) = c.meta.crd.as_ref() {
                let crd = serde_yaml::to_string(&crd).expect("failed to serialize CRD");
                print!("{}", crd);
            }
        }
    }

    async fn crd_apply(self) -> anyhow::Result<()> {
        let k = kube::Client::try_default()
            .await
            .context("failed to connect to cluster")?;
        let crd_api = Api::<CustomResourceDefinition>::all(k);
        for c in &self.controllers {
            if let Some(crd) = c.meta.crd.as_ref() {
                println!("Reconciling crd {}", crd.name());
                match crd_api.get(&crd.name()).await {
                    Ok(existing) => {
                        let report = crate::crds::is_subset_of(&existing, &crd, false);
                        report
                            .into_result()
                            .context("Error: can not safely replace existing crd")?;

                        println!("Updating crd {}", crd.name());
                        let mut crd = crd.clone();
                        crd.meta_mut().resource_version = existing.resource_version();
                        crd_api
                            .replace(&crd.name(), &Default::default(), &crd)
                            .await?;
                    }
                    Err(err)
                        if crate::errors::classify_kube(&err)
                            == crate::errors::ErrorClass::NotFound =>
                    {
                        println!("Creating crd {}", crd.name());
                        crd_api.create(&Default::default(), &crd).await?;
                    }
                    Err(err) => {
                        return Err(err).context("failed to get existing CRD");
                    }
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self, args))]
    async fn run(self, args: cli::Run) -> anyhow::Result<()> {
        let enabled_controllers = {
            let controllers = self
                .controllers
                .iter()
                .map(|c| c.meta.name.clone())
                .collect::<Vec<_>>();
            init::process_controller_filters(&controllers, &args.controllers)?
        };
        tracing::info!(enabled_controllers = ?enabled_controllers, "Selected controllers to run");
        tracing::info!("Connecting to Kubernetes");
        let client = kube::Client::try_default()
            .await
            .context("Failed to connect to kubernetes API")?;
        tracing::info!("Starting version skew checker");
        let version_skew_check_fut = {
            let client = client.clone();
            async move {
                loop {
                    let sleep_timeout =
                        match validate_api_server::check_api_server_version(&client).await {
                            Ok(_) => 3600,
                            Err(e) => {
                                tracing::warn!("Failed to validate api server version: {:#}", e);
                                10
                            }
                        };
                    tokio::time::sleep(Duration::from_secs(sleep_timeout)).await;
                }
            }
        };
        tokio::task::spawn(version_skew_check_fut);
        //tracing::info!("Discovering cluster APIs");
        //let discovery = Arc::new(Discovery::new(&client).await?);
        let watcher_set = crate::multiwatch::WatcherSet::new(client.clone());
        let watcher_set = Arc::new(watcher_set);
        let mut supervised = Vec::new();
        for controller in enabled_controllers {
            let dc = self
                .controllers
                .iter()
                .find(|c| c.meta.name == controller)
                .unwrap()
                .clone();
            let ctl = supervisor::supervise(
                dc,
                watcher_set.clone(),
                /*discovery.clone(),*/ client.clone(),
            );
            supervised.push(ctl);
        }
        {
            let mut cancel = Vec::new();
            for ctl in &supervised {
                cancel.push(ctl.get_cancellation_token());
            }
            tokio::task::spawn(async move {
                tracing::info!("Waiting for termination signal");
                match tokio::signal::ctrl_c().await {
                    Ok(_) => {
                        tracing::info!("Got termination signal");
                        for c in cancel {
                            c.cancel();
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to wait for termination signal: {:#}", e);
                    }
                }
            });
        }

        tracing::info!("Waiting for supervisors exit");
        for ctl in supervised {
            ctl.wait().await;
        }
        Ok(())
    }
}

/// Description of a controller
#[derive(Clone)]
struct ControllerDescription {
    name: String,
    crd: Option<CustomResourceDefinition>,
    watches: Vec<ApiResource>,
}

#[derive(Clone)]
struct DynController {
    meta: ControllerDescription,
    vtable: ControllerVtable,
}

impl DynController {
    fn validate(&self) {
        if let Some(crd) = self.meta.crd.as_ref() {
            let res = (self.vtable.api_resource)();
            assert_eq!(crd.spec.names.plural, res.plural);
            assert_eq!(crd.spec.names.kind, res.kind);
            assert_eq!(crd.spec.group, res.group);
            let has_version = crd.spec.versions.iter().any(|ver| ver.name == res.version);
            assert!(has_version);
        }
    }
}

#[derive(Clone)]
struct ControllerVtable {
    api_resource: fn() -> ApiResource,
    reconcile: fn(
        DynamicObject,
        cx: &mut ReconcileContext,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<ReconcileStatus>>,
}

impl ControllerVtable {
    fn of<C: Controller>() -> Self {
        ControllerVtable {
            api_resource: || ApiResource::erase::<C::Resource>(&C::resource_dynamic_type()),
            reconcile: |obj, cx| {
                async move {
                    // TODO: debug this
                    let obj = serde_json::to_string(&obj).unwrap();
                    let obj =
                        serde_json::from_str(&obj).context("failed to parse DynamicObject")?;

                    C::reconcile(cx, obj).await
                }
                .boxed()
            },
        }
    }
}

/// Context available to reconciler
pub struct ReconcileContext {
    client: kube::Client,
    applier: Applier,
    ws: Arc<WatcherSet>,
    namespace: Option<String>,
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

pub fn make_owner_reference<Owner: Resource>(
    owner: &Owner,
    dt: &Owner::DynamicType,
) -> OwnerReference {
    OwnerReference {
        api_version: Owner::api_version(dt).to_string(),
        block_owner_deletion: None,
        controller: Some(true),
        kind: Owner::kind(dt).to_string(),
        name: owner.name(),
        uid: owner.uid().expect("missing uid on persisted object"),
    }
}

pub fn downcast_dynamic_object<K: Resource<DynamicType = ()> + DeserializeOwned>(
    obj: &DynamicObject,
) -> anyhow::Result<K> {
    let obj = serde_json::to_value(obj)?;
    let obj = serde_json::from_value(obj)?;
    Ok(obj)
}

pub enum ReconcileStatus {
    /// Object is fully reconciled
    Done,
}

/// Trait, implemented by a controller
#[async_trait]
pub trait Controller {
    /// Resource which manages the controller behavior
    /// (e.g. Deployment for deployment controller)
    type Resource: Resource + DeserializeOwned + Send;

    /// Additional data for dynamic types
    fn resource_dynamic_type() -> <Self::Resource as Resource>::DynamicType;

    /// Reports some information to given collector
    fn describe<C: Collect>(collector: &C);

    /// Reconciles single object
    async fn reconcile(
        cx: &mut ReconcileContext,
        resource: Self::Resource,
    ) -> anyhow::Result<ReconcileStatus>;
}

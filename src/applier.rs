use anyhow::Context;
use kube::{
    api::{Patch, PatchParams, Resource, ResourceExt},
    Api,
};
use std::fmt::Debug;

/// Defines how exactly resources should be applied
#[derive(Clone)]
pub enum Strategy {
    /// Return an error if resource with the same name already exists
    Create,
    /// Overwrite resource if it exists
    Overwrite,
    /// Apply resource using server-side apply
    Apply { field_manager: String },
}

/// Hook observes each to-be-applied object
#[derive(Clone)]
pub struct Hook {
    func: fn(String),
}

impl Hook {
    pub fn print() -> Self {
        Hook {
            func: |repr| println!("{}", repr),
        }
    }

    pub fn null() -> Self {
        Hook { func: |_| () }
    }
}

/// Utility for creating objects of different types in cluster
#[derive(Clone)]
pub struct Applier {
    client: kube::Client,
    default_namespace: Option<String>,
    strategy: Strategy,
    hook: Hook,
}

impl Applier {
    /// Creates a new applier, connected to cluster
    pub fn new(
        client: kube::Client,
        default_namespace: Option<&str>,
        strategy: Strategy,
        hook: Hook,
    ) -> Self {
        Applier {
            client,
            default_namespace: default_namespace.map(|s| s.to_string()),
            strategy,
            hook,
        }
    }

    pub async fn do_apply<
        K: Resource<DynamicType = ()> + Clone + Debug + serde::de::DeserializeOwned + serde::Serialize,
    >(
        &self,
        resource: &K,
        api: Api<K>,
    ) -> anyhow::Result<K> {
        let repr = serde_yaml::to_string(&resource)?;
        println!("{}", repr);

        let client = api.clone().into_client();

        let created = match &self.strategy {
            Strategy::Create => api.create(&Default::default(), resource).await?,
            Strategy::Apply { field_manager } => {
                api.patch(
                    &resource.name(),
                    &PatchParams::apply(field_manager),
                    &Patch::Apply(resource),
                )
                .await?
            }
            Strategy::Overwrite => {
                let name = resource
                    .meta()
                    .name
                    .clone()
                    .context("name must be set when Overwrite strategy is in use")?;
                let _ = crate::delete::delete::<K>(&client, resource.namespace().as_deref(), &name)
                    .await;
                api.create(&Default::default(), &resource).await?
            }
        };
        Ok(created)
    }

    /// Applies a resource
    pub async fn apply<
        K: kube::api::Resource<DynamicType = ()>
            + Clone
            + Debug
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        mut resource: K,
    ) -> anyhow::Result<K> {
        let ns = resource
            .meta()
            .namespace
            .clone()
            .ok_or(())
            .or_else(|_| self.default_namespace.clone().ok_or(()))
            .map_err(|_| {
                anyhow::anyhow!("No namespace given and no default namespace configured")
            })?;
        resource.meta_mut().namespace = Some(ns.clone());

        let api = Api::namespaced(self.client.clone(), &ns);
        self.do_apply(&resource, api).await
    }

    /// Applies a cluster-scoped resource
    pub async fn apply_global<
        K: kube::api::Resource<DynamicType = ()>
            + Clone
            + Debug
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        resource: K,
    ) -> anyhow::Result<K> {
        let api = Api::all(self.client.clone());
        self.do_apply(&resource, api).await
    }
}

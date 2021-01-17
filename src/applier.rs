use kube::{
    api::{Meta, ObjectMeta, PatchParams},
    Api,
};
/// Defines how exactly resources should be applied
pub enum Strategy {
    /// Return an error if resource with the same name already exists
    Create,
    /// Overwrite resource if it exists
    Overwrite,
    /// Apply resource as a strategic merge patch
    Apply { field_manager: String },
}
/// Utility for creating objects of different types in cluster

pub struct Applier {
    client: kube::Client,
    default_namespace: String,
    strategy: Strategy,
}

impl Applier {
    /// Creates a new applier, connected to cluster
    pub fn new(client: kube::Client, default_namespace: &str, strategy: Strategy) -> Self {
        Applier {
            client,
            default_namespace: default_namespace.to_string(),
            strategy,
        }
    }

    pub async fn do_apply<
        K: Meta
            + k8s_openapi::Metadata<Ty = ObjectMeta>
            + Clone
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        resource: &K,
        api: Api<K>,
    ) -> anyhow::Result<()> {
        let repr = serde_yaml::to_string(&resource)?;
        println!("{}", repr);

        let client = api.clone().into_client();

        match &self.strategy {
            Strategy::Create => {
                api.create(&Default::default(), resource).await?;
            }
            Strategy::Apply { field_manager } => {
                api.patch(
                    &resource.name(),
                    &PatchParams::apply(field_manager),
                    serde_json::to_vec(&resource)?,
                )
                .await?;
            }
            Strategy::Overwrite => {
                let _ = crate::delete::delete::<K>(&client, resource.namespace().as_deref(), &resource.name()).await;
                api.create(&Default::default(), &resource).await?;
            }
        }
        Ok(())
    }

    /// Applies a resource
    pub async fn apply<
        K: kube::api::Meta
            + k8s_openapi::Metadata<Ty = ObjectMeta>
            + Clone
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        mut resource: K,
    ) -> anyhow::Result<()> {
        let meta = resource.metadata_mut();
        let ns = meta
            .namespace
            .get_or_insert_with(|| self.default_namespace.clone())
            .clone();

        let api = Api::namespaced(self.client.clone(), &ns);
        self.do_apply(&resource, api).await
    }

    /// Applies a cluster-scoped resource
    pub async fn apply_global<
        K: kube::api::Meta
            + k8s_openapi::Metadata<Ty = ObjectMeta>
            + Clone
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        resource: K,
    ) -> anyhow::Result<()> {
        let api = Api::all(self.client.clone());
        self.do_apply(&resource, api).await
    }
}

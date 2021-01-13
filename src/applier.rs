use anyhow::Context as _;
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

    /// Applies a resource
    pub async fn apply<
        K: kube::api::Meta
            + k8s_openapi::Metadata<Ty = kube::api::ObjectMeta>
            + Clone
            + serde::de::DeserializeOwned
            + serde::Serialize,
    >(
        &self,
        mut res: K,
    ) -> anyhow::Result<()> {
        let meta = res.metadata_mut();
        let ns = meta
            .namespace
            .get_or_insert_with(|| self.default_namespace.clone())
            .clone();

        let repr = serde_json::to_string(&res)?;
        println!("{}", repr);

        let api = kube::Api::<K>::namespaced(self.client.clone(), &ns);
        match &self.strategy {
            Strategy::Create => {
                api.create(&Default::default(), &res).await?;
            }
            Strategy::Apply { field_manager } => {
                api.patch(
                    res.metadata().name.as_ref().context("name missing")?,
                    &kube::api::PatchParams::apply(field_manager),
                    serde_json::to_vec(&res)?,
                )
                .await?;
            }
            Strategy::Overwrite => {}
        }

        Ok(())
    }
}

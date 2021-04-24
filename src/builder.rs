mod clusterrole;
mod configmap;
mod pod;

pub use clusterrole::ClusterRoleBuilder;
pub use configmap::ConfigMapBuilder;
pub use pod::PodBuilder;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::{ObjectMeta, Resource};

/// Type that knows how to build certain resource.
/// This trait is consumed by Builder, not by library users.
pub trait ConcreteBuilder<K> {
    fn new() -> Self;

    fn into_inner(self) -> K;

    fn get_mut(&mut self) -> &mut K;
}

/// Each resource needs special builder
pub trait Build: Resource<DynamicType = ()> + Sized {
    type ConcreteBuilder: ConcreteBuilder<Self>;
}

/// Utility for resource creation
pub struct Builder<K: Build>(K::ConcreteBuilder);

impl<K: Build> Builder<K> {
    pub fn new() -> Self {
        let mut b = K::ConcreteBuilder::new();
        *b.get_mut().meta_mut() = ObjectMeta {
            owner_references: Some(Vec::new()),
            ..Default::default()
        };
        Builder(b)
    }

    pub fn build(self) -> K {
        self.0.into_inner()
    }

    pub fn name(&mut self, name: &str) -> &mut Self {
        self.0.get_mut().meta_mut().name = Some(name.to_string());
        self
    }

    pub fn name_prefix(&mut self, prefix: &str) -> &mut Self {
        self.0.get_mut().meta_mut().generate_name = Some(prefix.to_string() + "-");
        self
    }

    pub fn owner(&mut self, owner_ref: OwnerReference) -> &mut Self {
        self.0
            .get_mut()
            .meta_mut()
            .owner_references
            .as_mut()
            .unwrap()
            .push(owner_ref);
        self
    }

    pub fn inner(&mut self) -> &mut K::ConcreteBuilder {
        &mut self.0
    }
}

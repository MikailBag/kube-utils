use k8s_openapi::{api::core::v1::ConfigMap, ByteString};
use std::collections::BTreeMap;
/// Utility for configmap creation
pub struct ConfigMapBuilder(ConfigMap);

impl crate::builder::ConcreteBuilder<ConfigMap> for ConfigMapBuilder {
    fn new() -> Self {
        ConfigMapBuilder(ConfigMap {
            binary_data: Some(BTreeMap::new()),

            ..Default::default()
        })
    }

    fn into_inner(self) -> ConfigMap {
        self.0
    }

    fn get_mut(&mut self) -> &mut ConfigMap {
        &mut self.0
    }
}

impl ConfigMapBuilder {
    pub fn immutable(&mut self) -> &mut Self {
        self.0.immutable = Some(true);
        self
    }

    pub fn add(&mut self, key: &str, data: Vec<u8>) -> &mut Self {
        self.0
            .binary_data
            .as_mut()
            .unwrap()
            .insert(key.to_string(), ByteString(data));
        self
    }
}

impl crate::builder::Build for ConfigMap {
    type ConcreteBuilder = ConfigMapBuilder;
}

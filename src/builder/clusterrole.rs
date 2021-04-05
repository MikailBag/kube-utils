use k8s_openapi::api::rbac::v1::{ClusterRole, PolicyRule};
use kube::api::Resource;

/// Utility for ClusterRole creation (can also be used to make Roles)
pub struct ClusterRoleBuilder(ClusterRole);

impl crate::builder::ConcreteBuilder<ClusterRole> for ClusterRoleBuilder {
    fn new() -> Self {
        ClusterRoleBuilder(ClusterRole {
            rules: Some(Vec::new()),
            ..Default::default()
        })
    }

    fn into_inner(self) -> ClusterRole {
        self.0
    }

    fn get_mut(&mut self) -> &mut ClusterRole {
        &mut self.0
    }
}

impl ClusterRoleBuilder {
    pub fn rule(&mut self, rule: PolicyRule) -> &mut Self {
        self.0.rules.as_mut().unwrap().push(rule);
        self
    }

    pub fn allow<K: Resource<DynamicType = ()>>(&mut self, verbs: &[&str]) -> &mut Self {
        let rule = PolicyRule {
            api_groups: Some(vec![K::group(&()).to_string()]),
            non_resource_urls: None,
            resource_names: None,
            resources: Some(vec![K::plural(&()).to_string()]),
            verbs: verbs.iter().map(ToString::to_string).collect(),
        };
        self.rule(rule)
    }
}

impl crate::builder::Build for ClusterRole {
    type ConcreteBuilder = ClusterRoleBuilder;
}

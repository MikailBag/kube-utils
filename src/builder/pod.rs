use k8s_openapi::api::core::v1::{
    ConfigMapVolumeSource, Container, Pod, PodSpec, Volume, VolumeMount,
};

/// Pod building
pub struct PodBuilder(Pod);

impl crate::builder::ConcreteBuilder<Pod> for PodBuilder {
    fn new() -> Self {
        PodBuilder(Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "main".to_string(),
                    args: Some(Vec::new()),
                    volume_mounts: Some(Vec::new()),
                    ..Default::default()
                }],
                volumes: Some(Vec::new()),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    fn into_inner(self) -> Pod {
        self.0
    }

    fn get_mut(&mut self) -> &mut Pod {
        &mut self.0
    }
}

impl PodBuilder {
    fn spec(&mut self) -> &mut PodSpec {
        self.0.spec.as_mut().unwrap()
    }

    fn container(&mut self) -> &mut Container {
        self.spec().containers.get_mut(0).unwrap()
    }

    fn volumes(&mut self) -> &mut Vec<Volume> {
        self.spec().volumes.as_mut().unwrap()
    }

    fn volume_mounts(&mut self) -> &mut Vec<VolumeMount> {
        self.container().volume_mounts.as_mut().unwrap()
    }

    pub fn image(&mut self, image: &str) -> &mut Self {
        self.container().image = Some(image.to_string());
        self
    }

    pub fn restart_never(&mut self) -> &mut Self {
        self.spec().restart_policy = Some("Never".to_string());
        self
    }

    pub fn args(&mut self, args: &[&str]) -> &mut Self {
        self.container().args = Some(args.iter().map(ToString::to_string).collect());
        self
    }

    pub fn mount_configmap(
        &mut self,
        mount_path: &str,
        configmap_name: &str,
        volume_name: &str,
    ) -> &mut Self {
        self.volumes().push(Volume {
            name: volume_name.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: Some(configmap_name.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });
        self.volume_mounts().push(VolumeMount {
            mount_path: mount_path.to_string(),
            name: volume_name.to_string(),
            ..Default::default()
        });

        self
    }
}

impl crate::builder::Build for Pod {
    type ConcreteBuilder = PodBuilder;
}

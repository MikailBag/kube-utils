use k8s_openapi::api::core::v1::{Container, Pod, PodSpec};

/// Pod building
pub struct PodBuilder(Pod);

impl crate::builder::ConcreteBuilder<Pod> for PodBuilder {
    fn new() -> Self {
        PodBuilder(Pod {
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "main".to_string(),
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
    fn container(&mut self) -> &mut Container {
        self.0.spec.as_mut().unwrap().containers.get_mut(0).unwrap()
    }

    pub fn image(&mut self, image: &str) -> &mut Self {
        self.container().image = Some(image.to_string());
        self
    }
}

impl crate::builder::Build for Pod {
    type ConcreteBuilder = PodBuilder;
}

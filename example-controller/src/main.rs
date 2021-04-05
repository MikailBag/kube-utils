use kube_utils::controller::{Collect, Controller, ControllerManager};
use async_trait::async_trait;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut controller_manager = ControllerManager::new();
    controller_manager.add::<ExecController>();
    controller_manager.main().await
}

struct ExecController;

#[async_trait]
impl Controller for ExecController {
    fn describe<C: Collect>(collector: &C) {
        collector.name("Exec")
    }
}

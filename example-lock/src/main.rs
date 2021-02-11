use anyhow::Context;
use kube_utils::lock::LockResult;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    tracing::info!("Connecting to kube");
    let k = kube::Client::try_default().await?;

    let identity = hostname().await?;

    tracing::info!(identity = identity.as_str(), "Generated identity");
    loop {
        tracing::info!("Acquiring lock");
        let params = kube_utils::lock::LockParams {
            namespace: "default".to_string(),
            name: "example".to_string(),
            lock_renew_interval: None,
            lock_timeout: None,
            conflict_sleep: None,
        };
        match kube_utils::lock::try_lock(
            k.clone(),
            params,
            serde_json::Value::String(identity.clone()),
        )
        .await?
        {
            LockResult::Leader(leader) => {
                tracing::info!("I'm leader");
                tracing::info!("Running 30 iterations of complex job");
                for i in 0..30 {
                    tracing::info!("Running computation #{}", i);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                tracing::info!("Stepping down");
                drop(leader);
            }
            LockResult::Follower {
                valid_for,
                leader_info,
            } => {
                tracing::info!("Leader: {:?}", leader_info);
                tracing::info!("Sleeping for {} seconds", valid_for.as_secs());
                tokio::time::sleep(valid_for).await;
            }
        }
    }
}

async fn hostname() -> anyhow::Result<String> {
    let mut cmd = tokio::process::Command::new("hostname");
    let out = cmd.output().await?;
    if !out.status.success() {
        anyhow::bail!("hostname failed");
    }
    let out = String::from_utf8(out.stdout).context("hostname output is not utf8")?;
    Ok(out.trim().to_string())
}

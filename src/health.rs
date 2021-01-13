//! Measuring health of some resources

use anyhow::Context as _;
use k8s_openapi::api::apps::v1::Deployment;

pub trait HealthSummary {
    fn healthy(&self) -> bool;
}

pub trait Health {
    type Summary: HealthSummary;
    fn health(&self) -> Self::Summary;
}

pub struct DeploymentSummary {
    /// minimum amount of pods are available
    pub available: bool,
    /// Possible error when parsing the deployment
    pub result: anyhow::Result<()>,
}

impl HealthSummary for DeploymentSummary {
    fn healthy(&self) -> bool {
        self.available
    }
}

impl Health for Deployment {
    type Summary = DeploymentSummary;
    fn health(&self) -> DeploymentSummary {
        fn priv_health(this: &Deployment) -> anyhow::Result<bool> {
            let status = this.status.as_ref().context(".status missing")?;
            let conditions = status
                .conditions
                .as_ref()
                .context(".status.conditions missing")?;
            for cond in conditions {
                if cond.type_ == "Available" {
                    return Ok(cond.status == "True");
                }
            }
            anyhow::bail!("Condition 'Available' missing");
        }
        match priv_health(self) {
            Ok(available) => DeploymentSummary {
                available,
                result: Ok(()),
            },
            Err(err) => DeploymentSummary {
                available: false,
                result: Err(err),
            },
        }
    }
}

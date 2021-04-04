use anyhow::Context;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::Api;

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct IsSubsetReport {
    /// Versions that were missing in the `lhs`.
    /// Does not intersect with `missing_deprecated_versions`
    pub missing_versions: Vec<String>,
    /// Versions that are different in `lhs` and `rhs`
    pub incompatible_versions: Vec<String>,
    /// If `allow_deprecated_removal` is true, this field will contain
    /// versions that are missing in `lhs` and deprecated in `rhs`.
    pub missing_deprecated_versions: Vec<String>,
}

#[derive(Debug)]
pub struct SubsetCheckFailedError(IsSubsetReport);

impl std::fmt::Display for SubsetCheckFailedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.0.missing_versions.is_empty() {
            write!(f, "missing versions:")?;
            for v in &self.0.missing_versions {
                write!(f, " {}", v)?;
            }
        }
        if !self.0.incompatible_versions.is_empty() {
            if !self.0.missing_versions.is_empty() {
                write!(f, ";")?;
            }
            write!(f, "incompatible versions:")?;
            for v in &self.0.missing_versions {
                write!(f, " {}", v)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for SubsetCheckFailedError {}

impl IsSubsetReport {
    pub fn is_ok(&self) -> bool {
        self.missing_versions.is_empty() && self.incompatible_versions.is_empty()
    }

    pub fn into_result(self) -> Result<(), SubsetCheckFailedError> {
        if self.is_ok() {
            Ok(())
        } else {
            Err(SubsetCheckFailedError(self))
        }
    }
}

/// Checks if the CRD `lhs` is subset of the CRD `b`.
/// Typical usage: `assert(is_subset(Foo::crd(), get_foo_crd_from_cluster()))`.
/// if `allow_deprecated_removal` is true `lhs` is allowed to omit versions that are
/// deprecated in `rhs`.
///
/// This function does not check metadata (e.g. it does not check names).
pub fn is_subset_of(
    lhs: &CustomResourceDefinition,
    rhs: &CustomResourceDefinition,
    allow_deprecated_removal: bool,
) -> IsSubsetReport {
    let expected_versions = rhs.spec.versions.as_slice();
    let mut report = IsSubsetReport {
        missing_versions: Vec::new(),
        incompatible_versions: Vec::new(),
        missing_deprecated_versions: Vec::new(),
    };
    for vers in expected_versions {
        let actual_version = lhs.spec.versions.iter().find(|v| v.name == vers.name);
        match actual_version {
            Some(v) => {
                if *v != *vers {
                    report.incompatible_versions.push(vers.name.clone());
                }
            }
            None => {
                if vers.deprecated == Some(true) && allow_deprecated_removal {
                    report.missing_deprecated_versions.push(vers.name.clone());
                } else {
                    report.missing_versions.push(vers.name.clone());
                }
            }
        }
    }
    report
}

pub async fn verify_compatible(
    k: &kube::Client,
    local: &CustomResourceDefinition,
    allow_deprecated_removal: bool,
) -> anyhow::Result<()> {
    let name = local
        .metadata
        .name
        .as_deref()
        .context("missing name on CRD")?;
    let crds_api = Api::<CustomResourceDefinition>::all(k.clone());
    let remote = crds_api.get(name).await.context("failed to fetch CRD deployed in cluster")?;
    let report = is_subset_of(local, &remote, allow_deprecated_removal);
    report.into_result().context("Deployed CRD is incompatible with local")?;
    Ok(())
}

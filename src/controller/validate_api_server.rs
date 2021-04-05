use anyhow::Context as _;

#[tracing::instrument(skip(k))]
pub(super) async fn check_api_server_version(k: &kube::Client) -> anyhow::Result<()> {
    k8s_openapi::k8s_if_1_11! { let local_version = 11; }
    k8s_openapi::k8s_if_1_12! { let local_version = 12; }
    k8s_openapi::k8s_if_1_13! { let local_version = 13; }
    k8s_openapi::k8s_if_1_14! { let local_version = 14; }
    k8s_openapi::k8s_if_1_15! { let local_version = 15; }
    k8s_openapi::k8s_if_1_16! { let local_version = 16; }
    k8s_openapi::k8s_if_1_17! { let local_version = 17; }
    k8s_openapi::k8s_if_1_18! { let local_version = 18; }
    k8s_openapi::k8s_if_1_19! { let local_version = 19; }
    k8s_openapi::k8s_if_1_20! { let local_version = 20; }
    let apiserver_version = k
        .apiserver_version()
        .await
        .context("failed to get apiserver version")?;

    if apiserver_version.major != "1" {
        tracing::error!(
            "Fatal version skew: apiserver major version is {}, which is not '1'",
            apiserver_version.major
        );
        return Ok(());
    }

    let apiserver_version: u32 = apiserver_version
        .minor
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse()
        .context("failed to parse apiserver minor version")?;
    tracing::info!(
        local = local_version,
        apiserver = apiserver_version,
        "Compared versions"
    );
    if apiserver_version < local_version {
        tracing::error!("Dangerous version skew: this binary was built against apiserver {}, but cluster version is {}", local_version, apiserver_version);
        return Ok(());
    }
    if apiserver_version > local_version {
        tracing::warn!(
            "Version skew: local = {}, actual = {}",
            local_version,
            apiserver_version
        );
        return Ok(());
    }
    Ok(())
}

use anyhow::Context as _;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;
use kube::config::{
    AuthInfo, Cluster, Context, Kubeconfig, NamedAuthInfo, NamedCluster, NamedContext,
};
use std::marker::PhantomData;

pub enum TrueTag {}
pub enum FalseTag {}

pub struct KubeconfigBuilder<HasCluster, HasUser>(Kubeconfig, PhantomData<(HasCluster, HasUser)>);

impl KubeconfigBuilder<FalseTag, FalseTag> {
    pub fn new() -> Self {
        KubeconfigBuilder(
            Kubeconfig {
                api_version: Some("v1".to_string()),
                kind: Some("Config".to_string()),
                preferences: None,
                clusters: vec![NamedCluster {
                    name: "main".to_string(),
                    cluster: Cluster {
                        server: "".to_string(),
                        certificate_authority: None,
                        certificate_authority_data: None,
                        insecure_skip_tls_verify: None,
                        extensions: None,
                    },
                }],
                auth_infos: vec![NamedAuthInfo {
                    name: "main".to_string(),
                    auth_info: AuthInfo {
                        username: None,
                        password: None,
                        token: None,
                        token_file: None,
                        client_certificate: None,
                        client_key: None,
                        client_certificate_data: None,
                        client_key_data: None,
                        impersonate: None,
                        impersonate_groups: None,
                        auth_provider: None,
                        exec: None,
                    },
                }],
                contexts: vec![NamedContext {
                    name: "main".to_string(),
                    context: Context {
                        cluster: "main".to_string(),
                        user: "main".to_string(),
                        namespace: None,
                        extensions: None,
                    },
                }],
                current_context: Some("main".to_string()),
                extensions: None,
            },
            PhantomData,
        )
    }
}

impl<HasCluster, HasUser> KubeconfigBuilder<HasCluster, HasUser> {
    fn ctx_mut(&mut self) -> &mut Context {
        &mut self.0.contexts[0].context
    }
    pub fn set_namespace(&mut self, ns: &str) -> &mut Self {
        self.ctx_mut().namespace = Some(ns.to_string());
        self
    }
}

impl<HasCluster> KubeconfigBuilder<HasCluster, FalseTag> {
    fn user_mut(&mut self) -> &mut AuthInfo {
        &mut self.0.auth_infos[0].auth_info
    }
    pub fn password(
        mut self,
        username: &str,
        password: &str,
    ) -> KubeconfigBuilder<HasCluster, TrueTag> {
        self.user_mut().username = Some(username.to_string());
        self.user_mut().password = Some(password.to_string());
        KubeconfigBuilder(self.0, PhantomData)
    }
    /// Configures authentication using PEM-encoded key and certificate
    pub fn x509(
        mut self,
        certificate: Vec<u8>,
        key: Vec<u8>,
    ) -> KubeconfigBuilder<HasCluster, TrueTag> {
        self.user_mut().client_certificate_data = Some(base64::encode(certificate));
        self.user_mut().client_key_data = Some(base64::encode(key));
        KubeconfigBuilder(self.0, PhantomData)
    }
}

impl<HasUser> KubeconfigBuilder<FalseTag, HasUser> {
    fn cluster_mut(&mut self) -> &mut Cluster {
        &mut self.0.clusters[0].cluster
    }
    pub fn set_cluster(
        mut self,
        cluster_url: &str,
        cluster_ca: &str,
    ) -> KubeconfigBuilder<TrueTag, HasUser> {
        self.cluster_mut().server = cluster_url.to_string();
        self.cluster_mut().certificate_authority_data = Some(base64::encode(&cluster_ca));
        KubeconfigBuilder(self.0, PhantomData)
    }
}

impl KubeconfigBuilder<TrueTag, TrueTag> {
    pub fn build(self) -> Kubeconfig {
        self.0
    }
}

/// Infers cluster URL. Only works in cluster.
pub async fn infer_cluster_url() -> anyhow::Result<String> {
    let mut host = std::env::var("KUBERNETES_SERVICE_HOST")?;
    if host.contains(':') {
        // IPv6 address
        host = format!("[{}]", host);
    }
    let port = std::env::var("KUBERNETES_SERVICE_PORT")?;
    Ok(format!("https://{}:{}", host, port))
}

/// Returns cluster certificate authority, as a PEM-encoded string.
pub async fn get_cluster_ca(k: &kube::Client) -> anyhow::Result<String> {
    let configmaps_api = Api::<ConfigMap>::namespaced(k.clone(), "default");
    let kube_root_ca_cm = configmaps_api
        .get("kube-root-ca.crt")
        .await
        .context("failed to fetch kube-root-ca.crt configmap in default namespace")?;
    let data = kube_root_ca_cm.data.context("configmap is empty")?;
    let crt = data.get("ca.crt").context("missing ca.crt key")?;
    Ok(crt.clone())
}

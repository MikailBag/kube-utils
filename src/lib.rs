pub mod applier;
pub mod delete;
pub mod health;
pub mod wait;
pub mod webhook;
pub mod storage;

use anyhow::Context as _;
use kube::Api;

pub async fn patch_with<K, F, Fut>(
    client: kube::Client,
    mut func: F,
    ns: Option<&str>,
    name: &str,
) -> anyhow::Result<()>
where
    K: k8s_openapi::Resource
        + k8s_openapi::Metadata<Ty = kube::api::ObjectMeta>
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + kube::api::Meta,
    F: FnMut(K) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<K>>,
{
    let api = match ns {
        Some(ns) => Api::<K>::namespaced(client, ns),
        None => Api::<K>::all(client),
    };
    let current = api
        .get(name)
        .await
        .context("failed to get current resource")?;
    let resource_version = current
        .metadata()
        .resource_version
        .as_ref()
        .context("missing resourceVersion")?
        .clone();
    let mut new = func(current).await.context("patch callback failed")?;
    new.metadata_mut().resource_version = Some(resource_version);

    api.replace(name, &Default::default(), &new).await?;
    Ok(())
}

pub fn make_reflector<
    K: kube::api::Meta + Clone + Send + Sync + serde::de::DeserializeOwned + 'static,
>(
    api: kube::Api<K>,
    cancel: tokio_util::sync::CancellationToken,
) -> kube_runtime::reflector::Store<K> {
    let watcher = kube_runtime::watcher(api, Default::default());
    let writer = kube_runtime::reflector::store::Writer::default();
    let store = writer.as_reader();
    let reflector = kube_runtime::reflector::reflector(writer, watcher);
    let fut = async move {
        tokio::pin!(reflector);
        while let Some(item) = futures_util::StreamExt::next(&mut reflector).await {
            if let Err(e) = item {
                tracing::warn!("reflection: error: {}", e);
            }
        }
    };
    tokio::task::spawn(async move {
        tokio::select! {
           _ = fut => (),
           _ = cancel.cancelled() => ()
        }
    });
    store
}

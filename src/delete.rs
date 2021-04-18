use anyhow::Context as _;
use kube::{api::WatchEvent, Api};
use std::fmt::Debug;
use tokio_stream::StreamExt;

pub async fn delete<
    K: kube::api::Resource<DynamicType = ()>
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Clone
        + Debug,
>(
    k: &kube::Client,
    ns: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let api: Api<K> = match ns {
        Some(ns) => Api::namespaced(k.clone(), ns),
        None => Api::all(k.clone()),
    };
    let original_object = api.get(name).await?;

    let delete_res = api
        .delete(name, &Default::default())
        .await
        .context("failed to delete")?;
    if delete_res.is_right() {
        return Ok(());
    }
    if let Err(err) = do_watch(&api, name).await {
        tracing::error!("Watch errored: {:#}", err);
    }

    // either object is finalized, or watch timed out

    for _ in 0..60 {
        let res = api.get(name).await;
        let gone = match res {
            Ok(obj) => obj.meta().uid != original_object.meta().uid,
            Err(_) => true,
        };
        if gone {
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    anyhow::bail!("timeout");
}

async fn do_watch<
    K: kube::api::Resource + serde::Serialize + serde::de::DeserializeOwned + Clone + Debug,
>(
    api: &Api<K>,
    name: &str,
) -> anyhow::Result<()> {
    let watch = api.watch(&kube::api::ListParams::default(), "0").await?;
    tokio::pin!(watch);
    while let Some(item) = watch.next().await {
        if let Ok(watch_event) = item {
            match watch_event {
                WatchEvent::Modified(obj) | WatchEvent::Deleted(obj) => {
                    let meta = obj.meta();
                    if meta.name.as_deref().context("missing name")? != name {
                        continue;
                    }
                    let gone = match meta.finalizers.as_ref() {
                        Some(fs) => fs.is_empty(),
                        None => true,
                    };
                    if gone {
                        break;
                    }
                }
                _ => (),
            }
        }
    }
    Ok(())
}

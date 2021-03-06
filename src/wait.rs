use anyhow::Context as _;
use futures::{future::BoxFuture, stream::StreamExt};
use kube::{api::ListParams, Api};
use std::fmt::Debug;

pub enum CallbackResponse<T> {
    /// Finish waiting
    Break(T),
    /// Continue waiting
    Continue,
}

pub trait Callback<K> {
    type Break;
    fn exec(
        &mut self,
        value: &K,
    ) -> BoxFuture<'static, anyhow::Result<CallbackResponse<Self::Break>>>;
}

pub struct CallbackFn<F>(F);

impl<K, Fut, Break, F> Callback<K> for CallbackFn<F>
where
    F: FnMut(&K) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<CallbackResponse<Break>>> + Send + 'static,
{
    type Break = Break;
    fn exec(
        &mut self,
        value: &K,
    ) -> BoxFuture<'static, anyhow::Result<CallbackResponse<Self::Break>>> {
        Box::pin((self.0)(value))
    }
}

pub fn callback_fn<K, Fut, Break, F>(func: F) -> CallbackFn<F>
where
    F: FnMut(&K) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<CallbackResponse<Break>>>,
{
    CallbackFn(func)
}

pub struct SimpleCallbackFn<F>(F);

impl<K, Break, F> Callback<K> for SimpleCallbackFn<F>
where
    F: FnMut(&K) -> Option<Break>,
    Break: Send + 'static,
{
    type Break = Break;
    fn exec(
        &mut self,
        value: &K,
    ) -> BoxFuture<'static, anyhow::Result<CallbackResponse<Self::Break>>> {
        let res = (self.0)(value);
        let res = match res {
            Some(r) => CallbackResponse::Break(r),
            None => CallbackResponse::Continue,
        };
        Box::pin(async move { Ok(res) })
    }
}

pub fn simple_callback_fn<K, Break, F>(func: F) -> SimpleCallbackFn<F>
where
    F: FnMut(&K) -> Option<Break>,
{
    SimpleCallbackFn(func)
}

#[tracing::instrument(skip(k, callback))]
pub async fn wait<
    K: kube::api::Resource<DynamicType = ()>
        + Clone
        + Debug
        + serde::de::DeserializeOwned
        + serde::Serialize
        + Send
        + Sync
        + 'static,
    C: Callback<K>,
>(
    k: &kube::Client,
    callback: &mut C,
    ns: Option<&str>,
    name: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<C::Break> {
    let api = match ns {
        Some(ns) => Api::<K>::namespaced(k.clone(), ns),
        None => Api::<K>::all(k.clone()),
    };
    let deadline_exceeded = tokio::time::sleep(timeout);
    let done = async move {
        let field_selector = format!("metadata.name={}", name);
        let watch =
            kube_runtime::watcher(api.clone(), ListParams::default().fields(&field_selector));
        let watch = kube_runtime::utils::try_flatten_applied(watch);
        let watch = watch.fuse();
        tokio::pin!(watch);
        loop {
            tracing::info!("Checking if the condition is met");
            let state = api.get(name).await?;
            let result = callback.exec(&state).await.context("Callback failed")?;
            if let CallbackResponse::Break(b) = result {
                break Ok(b);
            }
            while let Some(item) = watch.next().await {
                match item {
                    Ok(obj) => {
                        let meta = obj.meta();
                        if meta.name.as_ref().context("missing name")? == name {
                            break;
                        }
                    }
                    Err(err) => {
                        tracing::warn!("error: {:#}", err);
                    }
                }
            }
        }
    };

    tokio::select! {
        res = done => res,
        _ = deadline_exceeded => anyhow::bail!("Deadline exceeded")
    }
}

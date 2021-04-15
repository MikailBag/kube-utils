use futures::stream::StreamExt;
use kube::api::{Api, ApiResource, DynamicObject, Resource};
use kube_runtime::{reflector::store::Writer, watcher::Event};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::Instrument;

const WATCH_CHANNEL_CAPACITY: usize = 16;

struct WatchData {
    cache: Writer<DynamicObject>,
    subscribers: Vec<mpsc::Sender<Arc<DynamicObject>>>,
    sweep_on_next_iteration: bool,
}

impl WatchData {
    async fn send_item_normal(&mut self, item: Arc<DynamicObject>) {
        let mut cnt_errs = 0;
        for tx in &self.subscribers {
            if tx.send(item.clone()).await.is_err() {
                cnt_errs += 1;
            }
        }
        if cnt_errs * 2 > self.subscribers.len() {
            self.sweep_on_next_iteration = true;
        }
    }

    async fn send_item_with_sweep(&mut self, item: Arc<DynamicObject>) {
        let mut new_subscribers = Vec::new();
        for tx in std::mem::take(&mut self.subscribers) {
            if tx.send(item.clone()).await.is_ok() {
                new_subscribers.push(tx)
            }
        }
        self.subscribers = new_subscribers;
    }

    async fn send_item(&mut self, item: Arc<DynamicObject>) {
        if self.sweep_on_next_iteration {
            self.sweep_on_next_iteration = false;
            self.send_item_with_sweep(item).await;
        } else {
            self.send_item_normal(item).await;
        }
    }

    async fn on_event(&mut self, ev: Event<DynamicObject>) {
        // at first we deliver event to cache, and then notify
        // watchers
        self.cache.apply_watcher_event(&ev);
        for item in ev.into_iter_applied() {
            self.send_item(Arc::new(item)).await;
        }
    }
}

/// Multi-resource multi-consumer on top of kube-runtime's watcher
pub struct WatcherSet {
    client: kube::Client,
    data: RwLock<HashMap<ApiResource, Arc<Mutex<WatchData>>>>,
}

pub struct Watcher {
    rx: mpsc::Receiver<Arc<DynamicObject>>,
    initial: Vec<DynamicObject>,
}

impl Watcher {
    /// Returns next event
    pub async fn next(&mut self) -> Arc<DynamicObject> {
        match self.initial.pop() {
            Some(item) => Arc::new(item),
            None => self.rx.recv().await.expect("unexpected close"),
        }
    }
}

#[tracing::instrument(skip(wd, client))]
async fn background_watcher(wd: Arc<Mutex<WatchData>>, client: kube::Client, res: ApiResource) {
    let api = Api::<DynamicObject>::all_with(client, &res);
    let watch = kube_runtime::watcher(api, Default::default());
    tokio::pin!(watch);
    while let Some(ev) = watch.next().await {
        match ev {
            Ok(ev) => {
                tracing::debug!(event = ?ev, "delivering event");
                let mut wd = wd.lock().await;
                wd.on_event(ev).await;
            }
            Err(err) => {
                tracing::warn!("watch error: {:#}", err);
            }
        }
    }
    tracing::error!("watch closed");
}

impl WatcherSet {
    pub async fn watch_raw(&self, res: &ApiResource) -> Watcher {
        let mut data = self.data.write().await;
        let watch_data = match data.get_mut(&res) {
            Some(wd) => wd,
            None => {
                let cache = Writer::new(res.clone());
                let wd = WatchData {
                    sweep_on_next_iteration: false,
                    subscribers: Vec::new(),
                    cache,
                };
                let wd = Arc::new(Mutex::new(wd));
                data.insert(res.clone(), wd.clone());
                tokio::task::spawn(
                    background_watcher(wd, self.client.clone(), res.clone()).in_current_span(),
                );
                data.get_mut(res).unwrap()
            }
        };
        let mut watch_data = watch_data.lock().await;
        let (tx, rx) = mpsc::channel(WATCH_CHANNEL_CAPACITY);
        watch_data.subscribers.push(tx);
        let initial = watch_data.cache.as_reader().state();
        Watcher { rx, initial }
    }

    pub async fn watch<K: Resource<DynamicType = ()>>(&self) -> Watcher {
        self.watch_raw(&ApiResource::erase::<K>(&())).await
    }
}

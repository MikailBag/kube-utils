use crate::controller::ResourceInfo;
use kube::api::{Api, DynamicObject, Resource};
use kube_runtime::reflector::Store;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use parking_lot::Mutex;

/// Multi-resource multi-consumer on top of kube-runtime's watcher
pub(crate) struct MultiWatch {
    stores: Mutex<HashMap<ResourceInfo, Store<DynamicObject>>>,
}

pub(crate) struct Subscription {
    
}

impl MultiWatch {
    pub(crate) fn add_watch() {

    }
}
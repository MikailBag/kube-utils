use crate::controller::reconcile_queue::{Generation, QueueConfig, ReconcilationTask, TaskKey};
use std::{
    collections::{binary_heap::PeekMut, BinaryHeap, HashMap},
    time::{Duration, Instant},
};

/// Information that we remember about particular object.
struct ObjectInfo {
    /// Timestamp when object can become ready again.
    throttled_until: Instant,
    /// Last task received for this object (None if no tasks are in queue)
    task: Option<ReconcilationTask>,
}

impl Default for ObjectInfo {
    fn default() -> Self {
        ObjectInfo {
            throttled_until: Instant::now(),
            task: None,
        }
    }
}

// item that can be reconciled (it is just waiting for worker)
struct ReadyItem {
    gen: Generation,
    key: TaskKey,
}

// reverse order
impl Ord for ReadyItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.gen.cmp(&self.gen)
    }
}

impl PartialOrd for ReadyItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ReadyItem {}
impl PartialEq for ReadyItem {
    fn eq(&self, other: &Self) -> bool {
        self.gen == other.gen
    }
}

// item which is delayed for some reason
struct DelayedItem {
    enqueue_after: Instant,
    gen: Generation,
    key: TaskKey,
}

// reverse order
impl Ord for DelayedItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.enqueue_after.cmp(&self.enqueue_after)
    }
}

impl PartialOrd for DelayedItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for DelayedItem {}
impl PartialEq for DelayedItem {
    fn eq(&self, other: &Self) -> bool {
        self.gen == other.gen
    }
}

/// Queue itself
pub(super) struct RawQueue {
    cfg: QueueConfig,
    // sorted by generation to ensure FIFO
    ready: BinaryHeap<ReadyItem>,
    // sorted by expiration time
    delayed: BinaryHeap<DelayedItem>,
    objects: HashMap<TaskKey, ObjectInfo>,
}

impl RawQueue {
    pub(super) fn new(cfg: QueueConfig) -> Self {
        RawQueue {
            cfg,
            ready: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            objects: HashMap::new(),
        }
    }

    pub(super) fn add(&mut self, task: ReconcilationTask) {
        let info = self.objects.entry(task.key.clone()).or_default();
        info.task = Some(task.clone());

        if Instant::now() > info.throttled_until {
            self.ready.push(ReadyItem {
                gen: task.gen,
                key: task.key.clone(),
            });
        } else {
            task.span.in_scope(|| {
                tracing::debug!("Task is throttled");
            });
            self.delayed.push(DelayedItem {
                gen: task.gen,
                key: task.key.clone(),
                enqueue_after: info.throttled_until,
            });
        }
    }

    fn process_delayed(&mut self) {
        let now = Instant::now();
        loop {
            let head = match self.delayed.peek_mut() {
                Some(head) => {
                    if head.enqueue_after > now {
                        break;
                    }
                    head
                }
                None => break,
            };
            let ready = PeekMut::pop(head);
            let task = self.objects.get(&ready.key).and_then(|t| t.task.as_ref());
            let task = match task {
                Some(t) => t,
                None => {
                    // While this item was in queue, object was reconciled.
                    // Let's just throw this item away.
                    continue;
                }
            };
            task.span.in_scope(|| {
                tracing::debug!("Task is now ready");
            });

            let ready = ReadyItem {
                gen: ready.gen,
                key: ready.key,
            };
            self.ready.push(ready);
        }
    }

    pub(super) fn try_pop(&mut self) -> Option<ReconcilationTask> {
        self.process_delayed();
        loop {
            let item = self.ready.pop()?;
            let object_info = self
                .objects
                .get_mut(&item.key)
                .expect("ObjectInfo missing, but task was queued");
            // check if item is not stale
            if item.gen != object_info.task.as_ref().expect("Task stolen").gen {
                tracing::debug!("Ignoring stale heap item");
                continue;
            }
            object_info.throttled_until = Instant::now() + self.cfg.throttle;

            break Some(object_info.task.take().expect("Task stolen"));
        }
    }

    pub(super) fn readable(&mut self) -> bool {
        !self.ready.is_empty()
    }

    pub(super) fn park_timeout(&mut self) -> Option<Duration> {
        self.delayed
            .peek()
            .and_then(|next| next.enqueue_after.checked_duration_since(Instant::now()))
    }
}

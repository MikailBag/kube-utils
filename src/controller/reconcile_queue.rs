use std::{
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct QueueConfig {
    pub(crate) throttle: Duration,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct TaskKey {
    // must be ignored for cluster-scoped resources
    pub(crate) namespace: String,
    pub(crate) name: String,
}

impl ReconcilationTask {
    fn new(key: TaskKey, gen: Generation) -> Self {
        ReconcilationTask { key, gen }
    }
}

/// Generation is unique identifier for each ReconcilationTask.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct Generation(u64);

struct GenerationProducer {
    next: AtomicU64,
}

impl GenerationProducer {
    fn new() -> Self {
        GenerationProducer {
            next: AtomicU64::new(0),
        }
    }

    fn next(&self) -> Generation {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        if n == u64::MAX {
            panic!("Ran out of the generations");
        }
        Generation(n)
    }
}

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
struct Queue {
    cfg: QueueConfig,
    // sorted by generation to ensure FIFO
    ready: BinaryHeap<ReadyItem>,
    // sorted by expiration time
    delayed: BinaryHeap<DelayedItem>,
    objects: HashMap<TaskKey, ObjectInfo>,
}

impl Queue {
    fn new(cfg: QueueConfig) -> Self {
        Queue {
            cfg,
            ready: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            objects: HashMap::new(),
        }
    }

    #[tracing::instrument(skip(self))]
    fn add(&mut self, task: ReconcilationTask) {
        let info = self.objects.entry(task.key.clone()).or_default();

        if Instant::now() > info.throttled_until {
            self.ready.push(ReadyItem {
                gen: task.gen,
                key: task.key.clone(),
            });
        } else {
            tracing::debug!(generation = task.gen.0, key = ?task.key, "Task is throttled");
            self.delayed.push(DelayedItem {
                gen: task.gen,
                key: task.key.clone(),
                enqueue_after: info.throttled_until,
            });
        }

        info.task = Some(task);
    }

    fn process_delays(&mut self) {
        let now = Instant::now();
        loop {
            match self.delayed.peek() {
                Some(head) => {
                    if head.enqueue_after > now {
                        break;
                    }
                }
                None => break,
            }
            let ready = self
                .delayed
                .pop()
                .expect("it was checked that delayed queue is non-empty");
            tracing::debug!(generation = ready.gen.0, key = ?ready.key, "Task is now ready");
        }
    }

    #[tracing::instrument(skip(self))]
    fn pop(&mut self) -> Option<ReconcilationTask> {
        self.process_delays();
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
}

#[derive(Debug)]
struct ReconcilationTask {
    key: TaskKey,
    gen: Generation,
}

/// Can be used to send new tasks to queue.
/// Used by watchers.
#[derive(Clone)]
pub(crate) struct QueueSender {
    make_gen: Arc<GenerationProducer>,
    q: Arc<Mutex<Queue>>,
}

impl QueueSender {
    pub(crate) async fn send(&self, key: TaskKey) {
        let item = ReconcilationTask::new(key, self.make_gen.next());
        tracing::info!(generation = item.gen.0, object = ?item.key, "Enqueued task");
        let mut q = self.q.lock().await;
        q.add(item);
    }
}

/// Can be used to receive tasks from the queue.
/// Used by workers.
#[derive(Clone)]
pub(crate) struct QueueReceiver {
    q: Arc<Mutex<Queue>>,
}

impl QueueReceiver {
    /// Returns None when the queue is empty
    pub(crate) async fn recv(&self) -> Option<TaskKey> {
        let mut q = self.q.lock().await;
        let item = q.pop()?;
        tracing::info!(generation = item.gen.0, object = ?item.key, "Extracted task");
        Some(item.key)
    }
}

/// Creates a new queue for reconcilation tasks.
/// All arriving objects must have same TypeMeta.
pub(crate) fn queue(cfg: QueueConfig) -> (QueueSender, QueueReceiver) {
    let q = Queue::new(cfg);
    let q = Arc::new(Mutex::new(q));

    let make_gen = GenerationProducer::new();
    let make_gen = Arc::new(make_gen);

    (
        QueueSender {
            q: q.clone(),
            make_gen,
        },
        QueueReceiver { q },
    )
}

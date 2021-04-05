mod raw_queue;

use self::raw_queue::RawQueue;
use event_listener::Event;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
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
    fn new(key: TaskKey, gen: Generation, parent_span: tracing::Span) -> Self {
        let span = parent_span.in_scope(|| {
            tracing::info_span!(
                "Reconcillation task",
                id = gen.0,
                object_name = key.name.as_str(),
                object_namespace = key.namespace.as_str()
            )
        });
        ReconcilationTask { key, gen, span }
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
#[derive(Debug, Clone)]
struct ReconcilationTask {
    key: TaskKey,
    gen: Generation,
    span: tracing::Span,
}

struct Queue {
    raw: Mutex<RawQueue>,
    some_senders_are_alive: Mutex<Arc<()>>,
    // fires each time a new item is marked as ready
    // and each time a sender is dropped
    readable: Arc<Event>,
}

impl Queue {
    fn new(cfg: QueueConfig) -> Self {
        Queue {
            raw: Mutex::new(RawQueue::new(cfg)),
            // one sender will be immediately created
            some_senders_are_alive: Mutex::new(Arc::new(())),
            readable: Arc::new(Event::new()),
        }
    }

    async fn is_closed(&self) -> bool {
        Arc::get_mut(&mut *self.some_senders_are_alive.lock().await).is_some()
    }

    fn notify_senders(&self) {
        tracing::debug!("Waking up a sender");
        // no need to wakeup more than one worker for single task
        self.readable.notify_additional(1);
    }

    async fn add(&self, task: ReconcilationTask) {
        let mut inner = self.raw.lock().await;
        inner.add(task);
        if inner.readable() {
            self.notify_senders();
        }
    }

    #[tracing::instrument(skip(self))]
    async fn pop(&self) -> Option<ReconcilationTask> {
        loop {
            let readable = self.readable.listen();
            let park_timeout = {
                let mut raw = self.raw.lock().await;

                self.notify_senders();

                if let Some(item) = raw.try_pop() {
                    break Some(item);
                }

                if self.is_closed().await {
                    break None;
                }
                raw.park_timeout()
                    .unwrap_or_else(|| Duration::from_millis(1))
            };
            let park_fut = async move {
                tokio::select! {
                    _ = readable => {}
                    _ = tokio::time::sleep(park_timeout) => {}
                }
            };

            tracing::debug!("Queue is empty, parking");
            park_fut.await;
        }
    }
}

/// Can be used to send new tasks to queue.
/// Used by watchers.
#[derive(Clone)]
pub(crate) struct QueueSender {
    make_gen: Arc<GenerationProducer>,
    q: Arc<Queue>,
    alive: Option<Arc<()>>,
    notify_on_drop: Arc<Event>,
    span_for_tasks: tracing::Span,
}

impl Drop for QueueSender {
    fn drop(&mut self) {
        // decrement ref count before waking up senders
        let prev = self.alive.take();
        if prev.is_none() {
            panic!("Inconsistent")
        }

        // let's wake up all waiters.
        // if we were the last sender, they should realize
        // that the queue is closed
        self.notify_on_drop.notify(usize::MAX);
    }
}

impl QueueSender {
    pub(crate) async fn send(&self, key: TaskKey) {
        let item = ReconcilationTask::new(key, self.make_gen.next(), self.span_for_tasks.clone());
        let span = item.span.clone();
        tracing::debug!("Inserting key to queue {:?}", item.key);
        self.q.add(item).await;
        span.in_scope(|| {
            tracing::info!("Enqueued task");
        });
    }
}

/// Can be used to receive tasks from the queue.
/// Used by workers.
#[derive(Clone)]
pub(crate) struct QueueReceiver {
    q: Arc<Queue>,
}

impl QueueReceiver {
    /// Returns None when the queue is empty
    pub(crate) async fn recv(&self) -> Option<(TaskKey, tracing::Span)> {
        let item = self.q.pop().await?;
        item.span.in_scope(|| {
            tracing::info!("Extracted task");
        });

        Some((item.key, item.span))
    }
}

/// Creates a new queue for reconcilation tasks.
/// All arriving objects must have same TypeMeta.
pub(crate) fn queue(cfg: QueueConfig, tasks_span: tracing::Span) -> (QueueSender, QueueReceiver) {
    let mut q = Queue::new(cfg);
    let ev = q.readable.clone();
    let senders_alive = q.some_senders_are_alive.get_mut().clone();
    let q = Arc::new(q);

    let make_gen = GenerationProducer::new();
    let make_gen = Arc::new(make_gen);

    (
        QueueSender {
            q: q.clone(),
            make_gen,
            alive: Some(senders_alive),
            notify_on_drop: ev,
            span_for_tasks: tasks_span,
        },
        QueueReceiver { q },
    )
}

use std::{
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, FutureExt};
use pin_project::pin_project;
use vector_buffers::topology::channel::BufferReceiverStream;
use vector_core::{buffers::Acker, event::EventArray};

use crate::{config::ComponentKey, utilization::Utilization};

#[allow(clippy::large_enum_variant)]
pub(crate) enum TaskOutput {
    Source,
    Transform,
    /// Buffer of sink
    Sink(Utilization<BufferReceiverStream<EventArray>>, Acker),
    Healthcheck,
}

/// High level topology task.
#[pin_project]
pub(crate) struct Task {
    #[pin]
    inner: BoxFuture<'static, Result<TaskOutput, ()>>,
    key: ComponentKey,
    typetag: String,
}

impl Task {
    pub fn new<S, Fut>(key: ComponentKey, typetag: S, inner: Fut) -> Self
    where
        S: Into<String>,
        Fut: Future<Output = Result<TaskOutput, ()>> + Send + 'static,
    {
        Self {
            inner: inner.boxed(),
            key,
            typetag: typetag.into(),
        }
    }

    pub fn id(&self) -> &str {
        self.key.id()
    }

    pub fn typetag(&self) -> &str {
        &self.typetag
    }
}

impl Future for Task {
    type Output = Result<TaskOutput, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this: &mut Task = self.get_mut();
        this.inner.as_mut().poll(cx)
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.key.id().to_string())
            .field("typetag", &self.typetag)
            .finish()
    }
}

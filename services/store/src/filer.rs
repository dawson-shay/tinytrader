//! The store's filing owner (decision 0030): one task decides which write runs
//! and when, in place of a global lock. Writes are messages on a bounded channel;
//! with one writer (step 1) they run one at a time, in the order asked, as under
//! the lock they replace.
//!
//! - A **turn** is a write the asker runs itself: it holds the [`Turn`] for as long
//!   as it writes, and dropping it tells the filer it's done.
//! - A **partial-day filing** is run by the filer, grouped with the partial-day
//!   filings waiting right behind it (up to `GROUP`), so each day is written once.
//! - The scan's **settling** asks for a turn only when the filer is idle, and is
//!   skipped otherwise, so a merge waiting to be swapped in stays listed for the
//!   next scan.
//!
//! The channel holds `CAPACITY` writes; one asked past that is refused at once
//! ([`Busy`]), for its HTTP client to retry.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

/// Writes the filer may have waiting.
pub(crate) const CAPACITY: usize = 64;

/// A write the asker runs itself, until this is dropped.
pub(crate) struct Turn(#[allow(dead_code)] oneshot::Sender<()>);

/// The filer's queue is full.
#[derive(Debug)]
pub(crate) struct Busy;

/// A group of partial-day filings, filed by the closure the store gives the filer.
pub(crate) type Group<P> = Vec<P>;

enum Job<P> {
    Turn(oneshot::Sender<Turn>),
    Partial(P),
}

/// The handle every writer holds: the way to the filer.
#[derive(Clone)]
pub(crate) struct Filer<P> {
    tx: mpsc::Sender<Job<P>>,
    busy: Arc<AtomicBool>,
}

impl<P: Send + 'static> Filer<P> {
    /// Starts the filer. `file_group` files a group of partial-day filings (on a
    /// blocking thread), answering each itself.
    pub(crate) fn start(
        max_group: usize,
        file_group: impl Fn(Group<P>) + Send + Sync + 'static,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Job<P>>(CAPACITY);
        let busy = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&busy);
        let file_group = Arc::new(file_group);
        tokio::spawn(async move {
            let mut next: Option<Job<P>> = None;
            loop {
                let job = match next.take() {
                    Some(j) => j,
                    None => match rx.recv().await {
                        Some(j) => j,
                        None => return,
                    },
                };
                flag.store(true, Ordering::SeqCst);
                match job {
                    Job::Turn(ask) => {
                        let (done, finished) = oneshot::channel();
                        if ask.send(Turn(done)).is_ok() {
                            // Until the turn is dropped.
                            let _ = finished.await;
                        }
                    }
                    Job::Partial(first) => {
                        let mut group = vec![first];
                        while group.len() < max_group {
                            match rx.try_recv() {
                                Ok(Job::Partial(p)) => group.push(p),
                                Ok(other) => {
                                    next = Some(other);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        let f = Arc::clone(&file_group);
                        let _ = tokio::task::spawn_blocking(move || f(group)).await;
                    }
                }
                flag.store(next.is_some() || !rx.is_empty(), Ordering::SeqCst);
            }
        });
        Self { tx, busy }
    }

    /// Waits for a turn: every write asked before it has finished.
    ///
    /// # Errors
    /// [`Busy`] when the filer's queue is full, or the filer has stopped.
    pub(crate) async fn turn(&self) -> Result<Turn, Busy> {
        let (ask, answer) = oneshot::channel();
        self.tx.try_send(Job::Turn(ask)).map_err(|_| Busy)?;
        answer.await.map_err(|_| Busy)
    }

    /// A turn now, if nothing runs or waits; otherwise none (blocking: for the
    /// scan's thread).
    pub(crate) fn turn_if_idle(&self) -> Option<Turn> {
        if self.busy() {
            return None;
        }
        let (ask, answer) = oneshot::channel();
        self.tx.try_send(Job::Turn(ask)).ok()?;
        answer.blocking_recv().ok()
    }

    /// Hands the filer a partial-day filing, to file in a group.
    ///
    /// # Errors
    /// [`Busy`] when the filer's queue is full, or the filer has stopped.
    pub(crate) fn file_partial(&self, p: P) -> Result<(), Busy> {
        self.tx.try_send(Job::Partial(p)).map_err(|e| match e {
            TrySendError::Full(_) | TrySendError::Closed(_) => Busy,
        })
    }

    /// Whether a write runs or waits.
    pub(crate) fn busy(&self) -> bool {
        self.busy.load(Ordering::SeqCst)
    }

    /// Writes waiting in the queue.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> usize {
        self.tx.max_capacity() - self.tx.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::{CAPACITY, Filer};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type Filed = Arc<Mutex<Vec<Vec<u32>>>>;

    fn filer() -> (Filer<u32>, Filed) {
        let filed = Arc::new(Mutex::new(Vec::new()));
        let f = Arc::clone(&filed);
        let filer = Filer::start(16, move |g| f.lock().unwrap().push(g));
        (filer, filed)
    }

    /// Partial filings waiting behind a turn are filed as one group once it ends;
    /// while the turn is held, the filer is busy and an idle check gets no turn.
    #[tokio::test(flavor = "multi_thread")]
    async fn partials_behind_a_turn_are_one_group() {
        let (filer, filed) = filer();
        let turn = filer.turn().await.unwrap();
        for p in 1..=3 {
            filer.file_partial(p).unwrap();
        }
        assert!(filer.busy());
        let f = filer.clone();
        assert!(
            tokio::task::spawn_blocking(move || f.turn_if_idle())
                .await
                .unwrap()
                .is_none()
        );
        drop(turn);
        while filer.busy() || filer.queued() > 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(*filed.lock().unwrap(), vec![vec![1, 2, 3]]);
        let f = filer.clone();
        assert!(
            tokio::task::spawn_blocking(move || f.turn_if_idle())
                .await
                .unwrap()
                .is_some()
        );
    }

    /// Turns come in the order asked.
    #[tokio::test(flavor = "multi_thread")]
    async fn turns_come_in_order() {
        let (filer, _) = filer();
        let order = Arc::new(Mutex::new(Vec::new()));
        let first = filer.turn().await.unwrap();
        let mut waiting = Vec::new();
        for k in 0..3 {
            let (f, o) = (filer.clone(), Arc::clone(&order));
            waiting.push(tokio::spawn(async move {
                let _t = f.turn().await.unwrap();
                o.lock().unwrap().push(k);
            }));
            while filer.queued() < k + 1 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
        drop(first);
        for w in waiting {
            w.await.unwrap();
        }
        assert_eq!(*order.lock().unwrap(), vec![0, 1, 2]);
    }

    /// Past its capacity the filer refuses at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_queue_refuses() {
        let (filer, _) = filer();
        let _held = filer.turn().await.unwrap();
        for p in 0..CAPACITY {
            filer.file_partial(u32::try_from(p).unwrap()).unwrap();
        }
        assert!(filer.file_partial(999).is_err());
        assert!(filer.turn().await.is_err());
    }
}

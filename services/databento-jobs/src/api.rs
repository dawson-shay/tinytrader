//! The Databento calls this service makes, behind a trait so the purchase
//! paths can be tested against a fake Databento.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use databento::HistoricalClient;
use databento::dbn::Schema;
use databento::historical::batch::{
    BatchFileDesc, BatchJob, BatchJobShort, JobState, ListJobsParams, SubmitJobParams,
};
use databento::historical::metadata::{
    DatasetConditionDetail, DatasetRange, GetCostParams, GetDatasetConditionParams,
};

pub type Fut<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// A failed call, with the HTTP status Databento answered when it answered one:
/// decisions are made on the status, never on the message's text (which carries
/// request and job ids, whose digits can look like any status).
#[derive(Debug, Clone)]
pub struct CallError {
    pub status: Option<u16>,
    pub message: String,
}

impl CallError {
    /// A status that asking again won't change: a 4xx other than 408 and 429.
    #[must_use]
    pub fn permanent(&self) -> bool {
        self.status
            .is_some_and(|s| (400..500).contains(&s) && s != 408 && s != 429)
    }
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<String> for CallError {
    fn from(message: String) -> Self {
        Self {
            status: None,
            message,
        }
    }
}

impl From<databento::Error> for CallError {
    fn from(e: databento::Error) -> Self {
        let status = match &e {
            databento::Error::Api(a) => Some(a.status_code.as_u16()),
            databento::Error::Http(h) => h.status().map(|s| s.as_u16()),
            _ => None,
        };
        Self {
            status,
            message: e.to_string(),
        }
    }
}

/// A call whose failure carries its HTTP status.
pub type Checked<'a, T> = Pin<Box<dyn Future<Output = Result<T, CallError>> + Send + 'a>>;

pub trait Databento: Send + Sync {
    fn list_jobs<'a>(&'a self, p: &'a ListJobsParams) -> Fut<'a, Vec<BatchJobShort>>;
    fn job<'a>(&'a self, id: &'a str) -> Fut<'a, BatchJob>;
    fn files<'a>(&'a self, id: &'a str) -> Fut<'a, Vec<BatchFileDesc>>;
    fn submit<'a>(&'a self, p: &'a SubmitJobParams) -> Fut<'a, BatchJob>;
    /// The price in US dollars and the billable size in bytes.
    fn cost<'a>(&'a self, p: &'a GetCostParams) -> Fut<'a, (f64, u64)>;
    fn datasets(&self) -> Fut<'_, Vec<String>>;
    fn schemas<'a>(&'a self, dataset: &'a str) -> Fut<'a, Vec<Schema>>;
    fn range<'a>(&'a self, dataset: &'a str) -> Fut<'a, DatasetRange>;
    fn conditions<'a>(
        &'a self,
        p: &'a GetDatasetConditionParams,
    ) -> Fut<'a, Vec<DatasetConditionDetail>>;
    /// [`Self::job`], its failure with the HTTP status. By default, none.
    fn job_checked<'a>(&'a self, id: &'a str) -> Checked<'a, BatchJob> {
        Box::pin(async move { self.job(id).await.map_err(CallError::from) })
    }
    /// [`Self::files`], its failure with the HTTP status. By default, none.
    fn files_checked<'a>(&'a self, id: &'a str) -> Checked<'a, Vec<BatchFileDesc>> {
        Box::pin(async move { self.files(id).await.map_err(CallError::from) })
    }
}

/// Every job in every state over the account's whole history: what was
/// bought can't be judged from part of the list.
#[must_use]
pub fn every_job() -> ListJobsParams {
    ListJobsParams {
        states: Some(vec![
            JobState::Queued,
            JobState::Processing,
            JobState::Done,
            JobState::Expired,
        ]),
        since: Some(time::macros::datetime!(2000-01-01 0:00 UTC)),
    }
}

/// Databento itself, through a few clients: a call waits for a free one,
/// not behind every other call (pricing a long request makes hundreds).
pub struct Live {
    clients: Vec<tokio::sync::Mutex<HistoricalClient>>,
    next: std::sync::atomic::AtomicUsize,
}

impl Live {
    /// # Panics
    /// With no clients.
    #[must_use]
    pub fn new(clients: Vec<HistoricalClient>) -> Self {
        assert!(!clients.is_empty(), "at least one Databento client");
        Self {
            clients: clients.into_iter().map(tokio::sync::Mutex::new).collect(),
            next: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    async fn client(&self) -> tokio::sync::MutexGuard<'_, HistoricalClient> {
        for c in &self.clients {
            if let Ok(g) = c.try_lock() {
                return g;
            }
        }
        let i = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.clients.len();
        self.clients[i].lock().await
    }
}

macro_rules! call {
    ($self:ident, |$c:ident| $body:expr) => {
        Box::pin(async move {
            let mut $c = $self.client().await;
            $body.await.map_err(|e| e.to_string())
        })
    };
}

impl Databento for Live {
    fn list_jobs<'a>(&'a self, p: &'a ListJobsParams) -> Fut<'a, Vec<BatchJobShort>> {
        call!(self, |c| c.batch().list_jobs(p))
    }
    fn job<'a>(&'a self, id: &'a str) -> Fut<'a, BatchJob> {
        call!(self, |c| c.batch().get_job_details(id))
    }
    fn files<'a>(&'a self, id: &'a str) -> Fut<'a, Vec<BatchFileDesc>> {
        call!(self, |c| c.batch().list_files(id))
    }
    fn job_checked<'a>(&'a self, id: &'a str) -> Checked<'a, BatchJob> {
        Box::pin(async move {
            let mut c = self.client().await;
            c.batch().get_job_details(id).await.map_err(CallError::from)
        })
    }
    fn files_checked<'a>(&'a self, id: &'a str) -> Checked<'a, Vec<BatchFileDesc>> {
        Box::pin(async move {
            let mut c = self.client().await;
            c.batch().list_files(id).await.map_err(CallError::from)
        })
    }
    fn submit<'a>(&'a self, p: &'a SubmitJobParams) -> Fut<'a, BatchJob> {
        call!(self, |c| c.batch().submit_job(p))
    }
    fn cost<'a>(&'a self, p: &'a GetCostParams) -> Fut<'a, (f64, u64)> {
        Box::pin(async move {
            let mut c = self.client().await;
            let usd = c.metadata().get_cost(p).await.map_err(|e| e.to_string())?;
            let bytes = c
                .metadata()
                .get_billable_size(p)
                .await
                .map_err(|e| e.to_string())?;
            Ok((usd, bytes))
        })
    }
    fn datasets(&self) -> Fut<'_, Vec<String>> {
        call!(self, |c| c.metadata().list_datasets(None))
    }
    fn schemas<'a>(&'a self, dataset: &'a str) -> Fut<'a, Vec<Schema>> {
        call!(self, |c| c.metadata().list_schemas(dataset))
    }
    fn range<'a>(&'a self, dataset: &'a str) -> Fut<'a, DatasetRange> {
        call!(self, |c| c.metadata().get_dataset_range(dataset))
    }
    fn conditions<'a>(
        &'a self,
        p: &'a GetDatasetConditionParams,
    ) -> Fut<'a, Vec<DatasetConditionDetail>> {
        call!(self, |c| c.metadata().get_dataset_condition(p))
    }
}

/// Databento with every call bounded in time, so a hung call never holds
/// the purchase lock: a read gives up after `read`, a price after `cost`
/// (Databento can take minutes to price a long contract list), a submission
/// after `submit` (its outcome then unknown, so written `uncertain`).
pub struct Timed {
    pub inner: Arc<dyn Databento>,
    pub read: Duration,
    pub cost: Duration,
    pub submit: Duration,
}

impl Timed {
    #[must_use]
    pub fn new(inner: Arc<dyn Databento>) -> Self {
        Self {
            inner,
            read: Duration::from_secs(60),
            cost: Duration::from_secs(300),
            submit: Duration::from_secs(120),
        }
    }
}

fn within<'a, T: 'a>(d: Duration, f: Fut<'a, T>) -> Fut<'a, T> {
    Box::pin(async move {
        tokio::time::timeout(d, f)
            .await
            .map_err(|_| format!("no answer from Databento in {} s", d.as_secs_f64()))?
    })
}

fn within_checked<'a, T: 'a>(d: Duration, f: Checked<'a, T>) -> Checked<'a, T> {
    Box::pin(async move {
        tokio::time::timeout(d, f).await.map_err(|_| CallError {
            status: None,
            message: format!("no answer from Databento in {} s", d.as_secs_f64()),
        })?
    })
}

impl Databento for Timed {
    fn list_jobs<'a>(&'a self, p: &'a ListJobsParams) -> Fut<'a, Vec<BatchJobShort>> {
        within(self.read, self.inner.list_jobs(p))
    }
    fn job<'a>(&'a self, id: &'a str) -> Fut<'a, BatchJob> {
        within(self.read, self.inner.job(id))
    }
    fn files<'a>(&'a self, id: &'a str) -> Fut<'a, Vec<BatchFileDesc>> {
        within(self.read, self.inner.files(id))
    }
    fn job_checked<'a>(&'a self, id: &'a str) -> Checked<'a, BatchJob> {
        within_checked(self.read, self.inner.job_checked(id))
    }
    fn files_checked<'a>(&'a self, id: &'a str) -> Checked<'a, Vec<BatchFileDesc>> {
        within_checked(self.read, self.inner.files_checked(id))
    }
    fn submit<'a>(&'a self, p: &'a SubmitJobParams) -> Fut<'a, BatchJob> {
        within(self.submit, self.inner.submit(p))
    }
    fn cost<'a>(&'a self, p: &'a GetCostParams) -> Fut<'a, (f64, u64)> {
        within(self.cost, self.inner.cost(p))
    }
    fn datasets(&self) -> Fut<'_, Vec<String>> {
        within(self.read, self.inner.datasets())
    }
    fn schemas<'a>(&'a self, dataset: &'a str) -> Fut<'a, Vec<Schema>> {
        within(self.read, self.inner.schemas(dataset))
    }
    fn range<'a>(&'a self, dataset: &'a str) -> Fut<'a, DatasetRange> {
        within(self.read, self.inner.range(dataset))
    }
    fn conditions<'a>(
        &'a self,
        p: &'a GetDatasetConditionParams,
    ) -> Fut<'a, Vec<DatasetConditionDetail>> {
        within(self.read, self.inner.conditions(p))
    }
}

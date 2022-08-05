use crate::thread_pool::ThreadPool;
use crate::Result;
use std::thread;

/// a naive thread pool
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    /// do nothing
    fn new(_: usize) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool)
    }

    /// create a new thread for each spawned job.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

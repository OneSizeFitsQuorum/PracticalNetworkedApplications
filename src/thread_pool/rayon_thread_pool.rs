use crate::thread_pool::ThreadPool;

/// a thread pool wrapping rayon's threadPool
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    /// init rayon's threadPool
    fn new(num: usize) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool {
            pool: rayon::ThreadPoolBuilder::new().num_threads(num).build()?,
        })
    }

    /// spawn job to rayon's threadPool
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}

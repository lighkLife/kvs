use rayon;
use super::ThreadPool;
use crate::KvsError;
use crate::Result;

/// Wrapper of rayon::ThreadPool
pub struct RayonThreadPool {
    pool : rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self>
        where Self: Sized
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .map_err(|e| KvsError::StringError(format!("{}", e)))?;
        Ok(RayonThreadPool{pool})
    }

    fn spawn<F>(&self, f: F) where F: FnOnce() + Send + 'static {
        self.pool.spawn(f)
    }
}
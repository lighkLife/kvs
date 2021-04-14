use crate::Result;

mod naive;
mod shared_queue;
mod rayon;

pub use self::naive::NaiveThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
pub use self::rayon::RayonThreadPool;

/// a thread pool trait
pub trait ThreadPool {
    /// create a thread pool
    fn new(threads: u32) -> Result<Self>
        where Self: Sized;

    /// spawn a function
    fn spawn<F>(&self, f: F)
        where F: FnOnce() + Send + 'static;
}
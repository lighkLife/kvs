use crate::Result;

mod naive;
mod shared_queue;

pub use self::naive::NaiveThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;

/// a thread pool trait
pub trait ThreadPool {
    /// create a thread pool
    fn new(threads: u32) -> Result<Self>
        where Self: Sized;

    /// spawn a function
    fn spawn<F>(&self, f: F)
        where F: FnOnce() + Send + 'static;
}
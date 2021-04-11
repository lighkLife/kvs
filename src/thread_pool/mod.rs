use crate::Result;

mod naive;
pub use self::naive::NaiveThreadPool;

/// a thread pool trait
pub trait ThreadPool {
    /// create a thread pool
    fn new(threads: u32) -> Result<Self>
        where Self: Sized;

    /// spawn a function
    fn spawn<F>(&self, f: F)
        where F: FnOnce() + Send + 'static;
}
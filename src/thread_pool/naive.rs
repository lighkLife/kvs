use super::ThreadPool;
use crate::Result;
use std::thread;

/// a naive thread pool that creating a new thread for every job
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self>
        where Self: Sized
    {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, f: F)
        where F: FnOnce() + Send + 'static
    {
        thread::spawn(f);
    }
}
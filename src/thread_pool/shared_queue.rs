use crate::thread_pool::ThreadPool;
use crate::Result;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::thread;
use log::{warn, error, info, debug};

enum Message {
    NewJob(Job),
    Shutdown,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

/// a simple thread pool
pub struct SharedQueueThreadPool {
    sender: Sender<Message>,
    workers: Vec<Worker>,
}

/// thread pool worker
struct Worker {
    id: u32,
    receiver: Arc<Mutex<Receiver<Message>>>,
    handle: Option<JoinHandle<()>>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<Receiver<Message>>>) -> Worker {
        let receiver_clone = Arc::clone(&receiver);
        let handle = thread::spawn(move || run_job(id, receiver));
        Worker { id, receiver: receiver_clone, handle: Some(handle) }
    }
}

fn run_job(id: u32, receiver: Arc<Mutex<Receiver<Message>>>) {
    loop {
        let receiver = receiver.lock()
            .expect("get lock failed");
        let msg = receiver.recv()
            .expect("the corresponding channel has hung up.");
        match msg {
            Message::NewJob(job) => {
                job();
            }
            Message::Shutdown => {
                info!("ThreadPool work {} is shutting down", id);
                break;
            }
        };
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            error!("thread panicking");
            let id = self.id;
            let receiver = Arc::clone(&self.receiver);
            if let Err(e) = thread::Builder::new().spawn(move || run_job(id, receiver)) {
                error!("Failed to spawn a thread: {}", e);
            }
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> where Self: Sized {
        let (sender, receiver) = channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(threads as usize);
        for id in 0..threads {
            let receiver = Arc::clone(&receiver);
            workers.push(Worker::new(id, receiver));
        }
        Ok(SharedQueueThreadPool {
            sender,
            workers,
        })
    }

    fn spawn<F>(&self, f: F) where F: FnOnce() + Send + 'static {
        let job = Box::new(f);
        self.sender.send(Message::NewJob(job))
            .expect("The thread pool has no thread.");
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        debug!("SharedQueueThreadPool: send shutdown message to all workers");
        for _ in &mut self.workers {
            self.sender.send(Message::Shutdown).unwrap();
        }
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.join().unwrap();
            }
            debug!("SharedQueueThreadPool: shutdown worker {}", worker.id);
        }
    }
}
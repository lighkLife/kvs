use crate::thread_pool::ThreadPool;
use crate::Result;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use log::{error, info, debug};

enum Message {
    NewJob(Job),
    Shutdown,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

/// a simple thread pool
pub struct SharedQueueThreadPool {
    count: u32,
    sender: Sender<Message>,
}

/// thread pool worker
struct Worker {
    id: u32,
    active: bool,
    receiver: Arc<Mutex<Receiver<Message>>>,
}

impl Worker {
    /// create a worker
    fn new(id: u32, receiver: Arc<Mutex<Receiver<Message>>>) -> Worker {
        let receiver_clone = Arc::clone(&receiver);
        Worker { id, active: true, receiver: receiver_clone }
    }

    /// mark this worker is not active
    fn cancel(mut self) {
        debug!("work {} be canceled ", self.id);
        self.active = false;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.active {
            // only create a new thread for panic worker that is active
            if thread::panicking() {
                spawn_thread(self.id, Arc::clone(&self.receiver));
            }
        }
    }
}

fn spawn_thread(id: u32, receiver: Arc<Mutex<Receiver<Message>>>) {
    thread::Builder::new().spawn(move || {
        let worker = Worker::new(id, receiver);
        loop {
            let msg = {
                let receiver = worker.receiver.lock()
                    .expect("worker {} get lock failed");
                receiver.recv()
            };

            match msg {
                Ok(msg) => {
                    match msg {
                        Message::NewJob(job) => {
                            job();
                        }
                        Message::Shutdown => {
                            info!("ThreadPool work {} is shutting down", id);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("worker error {}", e);
                    break;
                }
            };
        }
        worker.cancel();
    }).expect("create thread failed");
}


impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> where Self: Sized {
        let (sender, receiver) = channel::<Message>();
        let receiver = Arc::new(Mutex::new(receiver));

        for id in 0..threads {
            let receiver = Arc::clone(&receiver);
            spawn_thread(id, receiver);
        }
        Ok(SharedQueueThreadPool {
            count: threads,
            sender,
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
        //todo graceful shutdown
        debug!("SharedQueueThreadPool: send shutdown message to all workers");
        for _ in 0..self.count {
            self.sender.send(Message::Shutdown).expect("send msg error");
        }
    }
}
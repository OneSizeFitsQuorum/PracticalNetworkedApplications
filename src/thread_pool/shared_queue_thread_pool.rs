use crate::thread_pool::ThreadPool;
use crate::Result;
use log::{debug, error};
use std::panic::AssertUnwindSafe;
use std::sync::mpsc::Receiver;
use std::sync::{mpsc, Arc, Mutex};
use std::{panic, thread};

/// a shared queue thread pool
pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Terminate,
}

impl ThreadPool for SharedQueueThreadPool {
    /// init num threads and related resources
    fn new(num: usize) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(num);
        for id in 0..num {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(SharedQueueThreadPool { workers, sender })
    }

    /// spawn the job to pools
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::NewJob(Box::new(job))).unwrap()
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        debug!("Sending terminate message to all workers.");

        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        debug!("Shutting down {} workers.", self.workers.len());

        for worker in &mut self.workers {
            debug!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Message>>>) -> Worker {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();
            match message {
                Message::NewJob(job) => {
                    debug!("{} receive a job", id);
                    if let Err(err) = panic::catch_unwind(AssertUnwindSafe(job)) {
                        error!("{} executes a job with error {:?}", id, err);
                    }
                }
                Message::Terminate => {
                    debug!("Worker {} terminated", id);
                    break;
                }
            }
        });
        Worker {
            id,
            thread: Some(thread),
        }
    }
}

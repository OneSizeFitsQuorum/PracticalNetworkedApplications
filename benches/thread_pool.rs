use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam_utils::sync::WaitGroup;
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{Client, KvServer, KvStore, Request, SledKvsEngine};
use log::{warn, LevelFilter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const ENTRY_COUNT: usize = 100;
const THREAD_COUNT: [usize; 4] = [1, 2, 4, 8];
static START: Once = Once::new();

fn write_queued_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("write_queued_kvstore");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = KvStore::open(dir.path()).unwrap();
            let server_pool = SharedQueueThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let value = value.clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::SET(key, value)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };
                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

fn read_queued_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("read_queued_kvstore");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = KvStore::open(dir.path()).unwrap();
            let server_pool = SharedQueueThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            for i in 0..ENTRY_COUNT {
                let mut write_client = Client::new(addr).unwrap();
                let key = keys[i].clone();
                let value = value.clone();
                write_client.request(&Request::SET(key, value)).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::GET(key)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };

                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

fn write_rayon_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("write_rayon_kvstore");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = KvStore::open(dir.path()).unwrap();
            let server_pool = RayonThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let value = value.clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::SET(key, value)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };
                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

fn read_rayon_kvstore(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("read_rayon_kvstore");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = KvStore::open(dir.path()).unwrap();
            let server_pool = RayonThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            for i in 0..ENTRY_COUNT {
                let mut write_client = Client::new(addr).unwrap();
                let key = keys[i].clone();
                let value = value.clone();
                write_client.request(&Request::SET(key, value)).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::GET(key)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };

                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("write_rayon_sledkvengine");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = SledKvsEngine::open(dir.path()).unwrap();
            let server_pool = RayonThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let value = value.clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::SET(key, value)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };
                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

fn read_rayon_sledkvengine(c: &mut Criterion) {
    START.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Info).init();
    });
    let mut group = c.benchmark_group("read_rayon_sledkvengine");
    for size in THREAD_COUNT.iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let addr = "127.0.0.1:4001";

            let dir = TempDir::new().unwrap();
            let eng = SledKvsEngine::open(dir.path()).unwrap();
            let server_pool = RayonThreadPool::new(size).unwrap();

            let is_stop = Arc::new(AtomicBool::new(false));

            let mut server = KvServer::new(eng, server_pool, Arc::clone(&is_stop));

            let handle = thread::spawn(move || {
                server.serve(&addr.to_owned()).unwrap();
            });

            let value = "value".to_owned();
            let keys: Vec<String> = (0..ENTRY_COUNT).map(|x| format!("key{}", x)).collect();
            let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

            thread::sleep(Duration::from_secs(1));

            for i in 0..ENTRY_COUNT {
                let mut write_client = Client::new(addr).unwrap();
                let key = keys[i].clone();
                let value = value.clone();
                write_client.request(&Request::SET(key, value)).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..ENTRY_COUNT {
                    let key = keys[i].clone();
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match Client::new(addr) {
                            Ok(mut client) => {
                                if let Err(err) = client.request(&Request::GET(key)) {
                                    warn!("request failed because {:?}", err);
                                }
                            }
                            Err(err) => {
                                warn!("init client failed because {:?}", err);
                            }
                        };

                        drop(wg);
                    });
                }
                wg.wait();
            });

            is_stop.store(true, Ordering::SeqCst);

            let _ = Client::new(addr);

            if let Err(err) = handle.join() {
                warn!("exit server failed because {:?}", err);
            }
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = write_queued_kvstore, read_queued_kvstore, write_rayon_kvstore, read_rayon_kvstore, write_rayon_sledkvengine, read_rayon_sledkvengine
}
criterion_main!(benches);

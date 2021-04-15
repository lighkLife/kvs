use criterion::{criterion_group, criterion_main, Criterion, BenchmarkGroup};
use kvs::{KvServer, KvsStoreEngine, KvStore, KvsClient, SledKvsEngine};
use tempfile::TempDir;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool, RayonThreadPool};
use std::thread;
use std::time::Duration;
use criterion::measurement::WallTime;

fn write_queued_kv_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_queued_kv_store");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_kv_store_server_with_queue(max_thread, 10000);
    run_write_bench(&mut group, max_thread, 10000);
    group.finish();
}


fn write_rayon_kv_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_kv_store");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_kv_store_server_with_rayon(max_thread, 20000);
    run_write_bench(&mut group, max_thread, 20000);
    group.finish();
}

fn read_queued_kv_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_queued_kv_store");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_kv_store_server_with_queue(max_thread, 30000);
    run_read_bench(&mut group, max_thread, 30000);
    group.finish();
}

fn read_rayon_kv_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_kv_store");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_kv_store_server_with_rayon(max_thread, 40000);
    run_read_bench(&mut group, max_thread, 40000);
    group.finish();
}

fn write_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_rayon_sled");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_sled_server_with_rayon(max_thread, 50000);
    run_write_bench(&mut group, max_thread, 50000);
    group.finish();
}

fn read_rayon_sled(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_rayon_sled");
    let max_thread = (num_cpus::get() * 2) as u32 + 1;
    start_sled_server_with_rayon(max_thread, 60000);
    run_read_bench(&mut group, max_thread, 60000);
    group.finish();
}




fn start_kv_store_server_with_queue(max_thread: u32, port: u32) {
    for thread_count in 1..max_thread {
        thread::spawn(move || {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir.path()).unwrap();
            let server = KvServer::new(KvsStoreEngine::new(kv_store));
            let pool = SharedQueueThreadPool::new(thread_count).unwrap();
            let addr = format!("127.0.0.1:{}", port + thread_count);
            server.start(&addr, pool).unwrap();
        });
    }
}

fn start_kv_store_server_with_rayon(max_thread: u32, port: u32) {
    for thread_count in 1..max_thread {
        thread::spawn(move || {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir.path()).unwrap();
            let server = KvServer::new(KvsStoreEngine::new(kv_store));
            let pool = RayonThreadPool::new(thread_count).unwrap();
            let addr = format!("127.0.0.1:{}", port + thread_count);
            server.start(&addr, pool).unwrap();
        });
    }
}

fn start_sled_server_with_rayon(max_thread: u32, port: u32) {
    for thread_count in 1..max_thread {
        thread::spawn(move || {
            let temp_dir = TempDir::new().unwrap();
            let db = sled::open(temp_dir.path()).unwrap();
            let server = KvServer::new(SledKvsEngine::new(db).unwrap());
            let pool = RayonThreadPool::new(thread_count).unwrap();
            let addr = format!("127.0.0.1:{}", port + thread_count);
            server.start(&addr, pool).unwrap();
        });
    }
}


fn run_write_bench(group: &mut BenchmarkGroup<WallTime>, max_thread: u32, port: u32) {
    for thread_count in 1..max_thread {
        let addr = format!("127.0.0.1:{}", port + thread_count);
        loop {
            if let Ok(mut client) = KvsClient::connect(&addr) {
                client.set("key".to_string(), "value".to_string()).unwrap();
                assert_eq!(Some("value".to_string()), client.get("key".to_string()).unwrap());
                println!("Start KvServer Success: {}", &addr);
                break;
            } else {
                println!("Wait KvServer {} starting...", &addr);
                thread::sleep(Duration::from_secs(1));
            }
        }

        group.bench_function(format!("{}-thread", thread_count), |b| {
            let mut client = KvsClient::connect(&addr).unwrap();
            b.iter(|| {
                for i in 0..1000 {
                    client.set(format!("key_{}", i), "value".to_string()).unwrap();
                }
            });
        });
    }
}

fn run_read_bench(group: &mut BenchmarkGroup<WallTime>, max_thread: u32, port: u32) {
    for thread_count in 1..max_thread {
        let addr = format!("127.0.0.1:{}", port + thread_count);
        loop {
            if let Ok(mut client) = KvsClient::connect(&addr) {
                client.set("key".to_string(), "value".to_string()).unwrap();
                assert_eq!(Some("value".to_string()), client.get("key".to_string()).expect("Get value failed from KvServer"));
                println!("Start KvServer Success: {}", &addr);
                break;
            } else {
                println!("Wait KvServer {} starting...", &addr);
                thread::sleep(Duration::from_secs(1));
            }
        }
        group.bench_function(format!("{}-thread", thread_count), |b| {
            let mut client = KvsClient::connect(&addr).unwrap();
            for i in 0..1000 {
                client.set(format!("key_{}", i), "value".to_string()).unwrap();
            }
            b.iter(|| {
                for i in 0..1000 {
                    client.get(format!("key_{}", i)).unwrap();
                }
            });
        });
    }
}



criterion_group!(server,
    write_queued_kv_store,
    write_rayon_kv_store,
    read_queued_kv_store,
    read_rayon_kv_store,
    write_rayon_sled,
    read_rayon_sled,
);
criterion_main!(server);
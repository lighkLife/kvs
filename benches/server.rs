use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvServer, KvsStoreEngine, KvStore, KvsClient};
use tempfile::TempDir;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};

fn write_queued_kv_store(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_queued_kv_store");
    for thread_count in 1..8 {
        group.bench_function(format!("{}-thread", thread_count), |b| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir.path()).unwrap();
            let server = KvServer::new(KvsStoreEngine::new(kv_store));
            let pool = SharedQueueThreadPool::new(thread_count).unwrap();
            let addr = format!("127.0.0.1:{}", 40000 + thread_count);
            server.start(&addr, pool);
            b.iter(|| {
                let mut client = KvsClient::connect(&addr).unwrap();
                for i in 0..1000 {
                    client.set(format!("key_{}", i), "value".to_string());
                }
            });
        });
    }
}



criterion_group!(server, write_queued_kv_store);
criterion_main!(server);
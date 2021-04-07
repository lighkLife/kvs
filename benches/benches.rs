use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use tempfile::TempDir;
use std::time::Duration;
use rand::distributions::{Alphanumeric, Standard};
use std::collections::HashMap;


fn bench_write(c: &mut Criterion) {
    c.bench_function("write_kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = KvStore::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        b.iter_batched(
            || gen_kv(&mut rng),
            |(key, val)| engine.set(key, val).unwrap(),
            BatchSize::SmallInput,
        )
    });
    c.bench_function("write_sled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledKvsEngine::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);

        b.iter_batched(
            || gen_kv(&mut rng),
            |(key, val)| engine.set(key, val).unwrap(),
            BatchSize::SmallInput,
        )
    });
}

fn bench_read(c: &mut Criterion) {
    c.bench_function("read_kvs", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = KvStore::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);
        let data = gen_data(&mut rng, &mut engine);

        b.iter_batched(
            || {
                let key = data.keys().choose(&mut rng).unwrap();
                let value = data.get(key).unwrap();
                (key.to_owned(), value.to_owned())
            },
            |(key, value)| assert_eq!(engine.get(key).unwrap().unwrap(), value),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("read_sled", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = SledKvsEngine::open(temp_dir.path()).unwrap();
        let mut rng = StdRng::seed_from_u64(0);
        let data = gen_data(&mut rng, &mut engine);

        b.iter_batched(
            || {
                let key = data.keys().choose(&mut rng).unwrap();
                let value = data.get(key).unwrap();
                (key.to_owned(), value.to_owned())
            },
            |(key, value)| assert_eq!(engine.get(key).unwrap().unwrap(), value),
            BatchSize::SmallInput,
        )
    });
}

fn gen_data(mut rng: impl Rng, engine: &mut impl KvsEngine) -> HashMap<String, String> {
    let mut data = HashMap::with_capacity(1000);
    for _ in 0..1000 {
        let (key, value) = gen_kv(&mut rng);
        data.insert(key.clone(), value.clone());
        engine.set(key, value).unwrap();
    }
    data
}

fn gen_kv(mut rng: impl Rng) -> (String, String) {
    let key_len = (&mut rng).gen_range(1..100001);
    let key = (&mut rng).sample_iter::<char, _>(&Standard).take(key_len).collect();

    let val_len = (&mut rng).gen_range(1..100001);
    let val = (&mut rng).sample_iter::<char, _>(&Standard).take(val_len).collect();

    (key, val)
}

criterion_group!(benches, bench_write, bench_read);
criterion_main!(benches);
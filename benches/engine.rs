use criterion::BatchSize::SmallInput;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use tempfile::TempDir;

fn write_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    let mut rng = &mut thread_rng();
    let range = (1..100000).choose_multiple(&mut rng, 1000).to_vec();
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).expect("unable to init KvStore");
                store
            },
            |store| {
                for i in &range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write KvStore");
                }
            },
            SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store =
                    SledKvsEngine::open(temp_dir.path()).expect("unable to init SledKvsEngine");
                store
            },
            |store| {
                for i in &range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write SledKvsEngine");
                }
            },
            SmallInput,
        )
    });
    group.finish()
}

fn read_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    let mut rng = &mut thread_rng();
    let write_range = (1..100000).choose_multiple(&mut rng, 1000).to_vec();
    let read_range = write_range.iter().choose_multiple(&mut rng, 300);
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store = KvStore::open(temp_dir.path()).expect("unable to init KvStore");
                for i in &write_range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write KvStore");
                }
                store
            },
            |store| {
                for i in &read_range {
                    store
                        .get(format!("key{}", i))
                        .expect("unable to read KvStore");
                }
            },
            SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir =
                    TempDir::new().expect("unable to create temporary working directory");
                let store =
                    SledKvsEngine::open(temp_dir.path()).expect("unable to init SledKvsEngine");
                for i in &write_range {
                    store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("unable to write SledKvsEngine");
                }
                store
            },
            |store| {
                for i in &read_range {
                    store
                        .get(format!("key{}", i))
                        .expect("unable to read SledKvsEngine");
                }
            },
            SmallInput,
        )
    });
    group.finish()
}

criterion_group!(benches, write_benchmark, read_benchmark);
criterion_main!(benches);

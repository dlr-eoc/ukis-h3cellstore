use std::collections::HashMap;

use criterion::{criterion_group, criterion_main, Criterion};
use h3ron::{H3Cell, Index};

use bamboo_h3_int::{ColVec, ColumnSet};

fn generate_mixed_resolution_h3indexes() -> Vec<u64> {
    let start_index = H3Cell::new(0x89283080ddbffff_u64);
    start_index
        .k_ring(30)
        .iter()
        .flat_map(|index| index.get_children(10))
        .chain(
            start_index
                .k_ring(10)
                .iter()
                .flat_map(|index| index.get_children(11)),
        )
        .map(|index| index.h3index())
        .collect()
}

fn build_columnset(h3indexes: Vec<u64>) -> ColumnSet {
    // a sample value column to attach to the indexes
    let values: Vec<_> = h3indexes.iter().map(|i| i.rem_euclid(13) as u8).collect();

    let mut outmap = HashMap::new();
    outmap.insert("h3index".to_string(), ColVec::U64(h3indexes));
    outmap.insert("value".to_string(), ColVec::U8(values));

    ColumnSet::from(outmap)
}

fn generate_mixed_resolution_columnset() -> ColumnSet {
    build_columnset(generate_mixed_resolution_h3indexes())
}

fn generate_one_resolution_columnset() -> ColumnSet {
    let start_index = H3Cell::new(0x89283080ddbffff_u64);
    build_columnset(
        start_index
            .k_ring(150)
            .iter()
            .map(|i| i.h3index())
            // worsen the possible compaction by creating holes
            .filter(|h3i| h3i.rem_euclid(12) == 0)
            .collect()
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("columnset");
    group.sample_size(30);

    group.bench_function("split_by_resolution", |bencher| {
        let columnset_mixed_resolution = generate_mixed_resolution_columnset();
        bencher.iter(|| {
            columnset_mixed_resolution
                .split_by_resolution(&"h3index".to_string(), false)
                .unwrap()
        })
    });

    group.bench_function("to_compacted", |bencher| {
        let columnset_one_resolution = generate_one_resolution_columnset();
        bencher.iter(|| {
            columnset_one_resolution
                .to_compacted(&"h3index".to_string())
                .unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

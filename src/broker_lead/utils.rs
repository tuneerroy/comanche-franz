use std::{collections::HashMap, sync::MutexGuard};

use crate::ServerId;

pub fn get_brokers_with_least_partitions(
    broker_partition_count: &MutexGuard<HashMap<ServerId, usize>>,
    partition_count: usize,
) -> Vec<ServerId> {
    let mut min_heap = std::collections::BinaryHeap::new();
    eprintln!("broker_partition_count: {:?}", broker_partition_count);
    eprintln!("partition_count: {:?}", partition_count);
    for (broker, count) in broker_partition_count.iter() {
        min_heap.push((*count, broker));
    }
    eprintln!("min_heap: {:?}", min_heap);
    let mut res = Vec::new();
    for _ in 0..partition_count {
        let (count, broker) = min_heap.pop().unwrap();
        min_heap.push((count + 1, broker));
        res.push(*broker);
    }
    res
}

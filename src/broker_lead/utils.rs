use std::{collections::HashMap, sync::MutexGuard};

use crate::ServerId;

pub fn get_brokers_with_least_partitions(
    broker_partition_count: &MutexGuard<HashMap<ServerId, usize>>,
    partition_count: usize,
) -> Vec<ServerId> {
    let mut min_heap = std::collections::BinaryHeap::new();
    for (broker, count) in broker_partition_count.iter() {
        min_heap.push((count, broker));
    }
    min_heap
        .into_sorted_vec()
        .iter()
        .take(partition_count)
        .map(|(_, broker)| (*broker).clone())
        .collect()
}

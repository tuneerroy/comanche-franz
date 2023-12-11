use std::{
    collections::{HashMap, HashSet},
    thread,
};

use crate::broker::Broker;

use self::listeners;
// use Broker;

pub struct BrokerLeader {
    addr: u16,
    // track which partitions are on which brokers
    topic_to_partitions: HashMap<String, Vec<u16>>,
    // track how many partitions each broker has
    broker_partition_count: HashMap<u16, usize>,
    // track which consumers are subscribed to which topics
    topic_to_consumers: HashMap<String, HashSet<u16>>,
    // number of partitions per topic
    partition_count: usize,
}

fn get_brokers_with_least_partitions(
    broker_partition_count: &HashMap<u16, usize>,
    partition_count: usize,
) -> Vec<u16> {
    let mut min_heap = std::collections::BinaryHeap::new();
    for (broker, count) in broker_partition_count.iter() {
        min_heap.push((count, broker));
    }
    min_heap
        .drain(..partition_count)
        .map(|(_, broker)| *broker)
        .collect()
}

impl BrokerLeader {
    pub fn new(addr: u16, broker_count: usize, partition_count: usize) -> BrokerLeader {
        let mut broker_ports = Vec::new();
        for i in 0..broker_count {
            thread::spawn(|| {
                let addr = addr + (i as u16) + 1;
                broker_ports.push(addr);
                Broker::new(addr);
                Broker::listen()
            });
        }

        BrokerLeader {
            addr,
            topic_to_partitions: HashMap::new(),
            broker_partition_count: HashMap::new(),
            topic_to_consumers: HashMap::new(),
            partition_count,
        }
    }

    pub async fn listen(&mut self) {
        let producer_add_topic = warp::post()
            .and(warp::path("add_topic"))
            .and(warp::body::json())
            .map(|body: ProducerAddsTopic| {
                if self.topic_to_partitions.contains_key(&body.topic) {
                    return warp::reply::json(&"Topic already exists");
                }

                let brokers = get_brokers_with_least_partitions(
                    &self.broker_partition_count,
                    self.partition_count,
                );
                self.topic_to_partitions
                    .insert(body.topic.clone(), brokers.clone());
                slef.topic_to_consumers
                    .insert(body.topic.clone(), HashSet::new());

                for broker in brokers {
                    *self.broker_partition_count.entry(broker).or_insert(1) += 1;
                }

                warp::reply::json(&self.topic_to_partitions[&body.topic])
            });

        let producer_remove_topic = warp::delete()
            .and(warp::path("remove_topic"))
            .and(warp::body::json())
            .map(|body: ProducerRemovesTopic| {
                if !self.topic_to_partitions.contains_key(&body.topic) {
                    return warp::reply::json(&"Topic does not exist");
                }

                let brokers = self.topic_to_partitions.remove(&body.topic).unwrap();
                for broker in brokers {
                    *self.broker_partition_count.entry(broker).or_insert(1) -= 1;
                }
                self.topic_to_consumers.remove(&body.topic);

                warp::reply::json(&"OK")
            });

        let consumer_subscribe = warp::post()
            .and(warp::path("subscribe"))
            .and(warp::body::json())
            .map(|body: ConsumerSubscribes| {
                if !self.topic_to_partitions.contains_key(&body.topic) {
                    return warp::reply::json(&"Topic does not exist");
                }

                self.topic_to_consumers
                    .entry(body.topic.clone())
                    .or_insert(HashSet::new())
                    .insert(body.consumer_id);
                warp::reply::json(&self.topic_to_partitions[&body.topic])
            });

        let consumer_unsubscribe = warp::delete()
            .and(warp::path("unsubscribe"))
            .and(warp::body::json())
            .map(|body: ConsumerUnsubscribes| {
                if !self.topic_to_partitions.contains_key(&body.topic) {
                    return warp::reply::json(&"Topic does not exist");
                }

                self.topic_to_consumers
                    .entry(body.topic.clone())
                    .or_insert(HashSet::new())
                    .remove(&body.consumer_id);
                warp::reply::json(&"OK")
            });

        warp::serve(
            producer_add_topic
                .or(producer_remove_topic)
                .or(consumer_subscribe)
                .or(consumer_unsubscribe),
        )
        .run(([127, 0, 0, 1], self.addr))
        .await;
    }

    pub fn num_partitions(topic: &str) {
        topic_to_partitions[topic].len()
    }
}

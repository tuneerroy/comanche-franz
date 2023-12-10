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
    // track subscriptions for the sake of notifying consumers when changes
    // to partitions
    topic_to_consumers: HashMap<String, HashSet<u16>>,
    // list of brokers, just so they don't get freed
    // threads: Vec<JoinHandle<>>,
    //brokers: Vec<u16>, // TODO: CHANGE THIS TO ACTUALLY BE BROKERS
    // track how many partitions each broker has
    broker_partition_count: HashMap<u16, usize>,

    broker_ports: Vec<u16>,
}

impl BrokerLeader {
    pub fn new(addr: u16, broker_count: usize) -> BrokerLeader {
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
            topic_to_consumers: HashMap::new(),
            // brokers: (addr..addr + broker_count as u16).collect(),
            broker_partition_count: HashMap::new(),
            broker_ports,
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

                self.topic_to_partitions
                    .insert(body.topic.clone(), self.broker_ports.clone());
                self.topic_to_consumers
                    .insert(body.topic.clone(), HashSet::new());
                for broker in self.broker_ports.iter() {
                    *self.broker_partition_count.entry(*broker).or_insert(0) += 1;
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

                self.topic_to_partitions.remove(&body.topic);
                self.topic_to_consumers.remove(&body.topic);
                for broker in self.broker_ports.iter() {
                    *self.broker_partition_count.entry(*broker).or_insert(1) -= 1;
                }
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

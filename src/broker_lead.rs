use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    thread,
};

use warp::Filter;

use crate::{
    broker::Broker, consumer_group::ConsumerGroup, PartitionId, PartitionInfo, ServerId, Topic,
};

mod utils;

pub struct BrokerLead {
    addr: ServerId,
    // track which topics
    topic_to_partitions: Arc<Mutex<HashMap<String, Vec<PartitionInfo>>>>,
    // how many producers producing a said topic
    topic_to_producer_count: Arc<Mutex<HashMap<String, usize>>>,
    // track how many partitions each broker has
    broker_partition_count: Arc<Mutex<HashMap<ServerId, usize>>>,
    // track which consumers are subscribed to which topics
    consumers_to_groups: Arc<Mutex<HashMap<ServerId, ConsumerGroup>>>,
    // number of partitions per topic
    partition_count: usize,
    // consumer group coordinator
    // mapping of consumer to consumer group
    // consumer group needs to know which partitions it's responsible for
    // consumer group needs

    // consumer requests to join a group
    // consumser requests to leave a group
    // consumer subscribes to a topic
    // consumer unsubscribes from a topic
    // if any of the above happens, needs to reach out to consumer
    // consumer requests a message
}

impl BrokerLead {
    pub fn new(addr: u16, broker_count: usize, partition_count: usize) -> BrokerLead {
        let broker_partition_count = Arc::new(Mutex::new(HashMap::new()));
        for i in 0..broker_count {
            thread::spawn({
                let broker_partition_count = broker_partition_count.clone();
                move || async move {
                    let addr = addr + (i as u16) + 1;
                    let mut broker = Broker::new(addr);
                    broker_partition_count
                        .lock()
                        .unwrap()
                        .insert(addr, partition_count);
                    Broker::listen(&mut broker).await;
                }
            });
        }

        BrokerLead {
            addr,
            topic_to_partitions: Arc::new(Mutex::new(HashMap::new())),
            topic_to_producer_count: Arc::new(Mutex::new(HashMap::new())),
            broker_partition_count: broker_partition_count.clone(),
            consumers_to_groups: Arc::new(Mutex::new(HashMap::new())),
            partition_count,
        }
    }

    pub async fn listen(&mut self) {
        let producer_add_topic = warp::post()
            .and(warp::path("topics"))
            .and(warp::body::json())
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let topic_to_producer_count = self.topic_to_producer_count.clone();
                let broker_partition_count = self.broker_partition_count.clone();
                let partition_count = self.partition_count;
                move |topic: Topic| {
                    let mut topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut topic_to_producer_count = topic_to_producer_count.lock().unwrap();
                    let broker_partition_count = broker_partition_count.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        let broker_server_ids = utils::get_brokers_with_least_partitions(
                            &broker_partition_count,
                            partition_count,
                        );
                        topic_to_partitions.insert(
                            topic.clone(),
                            broker_server_ids
                                .iter()
                                .enumerate()
                                .map(|(i, broker_server_id)| {
                                    let partition_id = PartitionId::new(topic.clone(), i);
                                    PartitionInfo::new(partition_id, *broker_server_id)
                                })
                                .collect(),
                        );
                    }

                    topic_to_producer_count
                        .entry(topic.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    warp::reply::json(&topic_to_partitions[&topic].clone())
                }
            });

        // let producer_remove_topic = warp::delete()
        //     .and(warp::path!("topics" / String))
        //     .and(warp::body::json())
        //     .map(|body: ProducerRemovesTopic| {
        //         if !self.topic_to_partitions.contains_key(&body.topic) {
        //             return warp::reply::json(&"Topic does not exist");
        //         }

        //         let producer_count = self.topic_to_producer_count.entry(body.topic.clone());

        //         let brokers = self.topic_to_partitions.remove(&body.topic).unwrap();
        //         for broker in brokers {
        //             *self.broker_partition_count.entry(broker).or_insert(1) -= 1;
        //         }
        //         self.topic_to_consumers.remove(&body.topic);

        //         warp::reply::json(&"OK")
        //     });

        // let consumer_subscribe = warp::post()
        //     .and(warp::path("subscribe"))
        //     .and(warp::body::json())
        //     .map(|body: ConsumerSubscribes| {
        //         if !self.topic_to_partitions.contains_key(&body.topic) {
        //             return warp::reply::json(&"Topic does not exist");
        //         }

        //         self.topic_to_consumers
        //             .entry(body.topic.clone())
        //             .or_insert(HashSet::new())
        //             .insert(body.consumer_id);
        //         warp::reply::json(&self.topic_to_partitions[&body.topic])
        //     });

        // let consumer_unsubscribe = warp::delete()
        //     .and(warp::path("unsubscribe"))
        //     .and(warp::body::json())
        //     .map(|body: ConsumerUnsubscribes| {
        //         if !self.topic_to_partitions.contains_key(&body.topic) {
        //             return warp::reply::json(&"Topic does not exist");
        //         }

        //         self.topic_to_consumers
        //             .entry(body.topic.clone())
        //             .or_insert(HashSet::new())
        //             .remove(&body.consumer_id);
        //         warp::reply::json(&"OK")
        //     });

        // warp::serve(
        //     producer_add_topic
        //         .or(producer_remove_topic)
        //         .or(consumer_subscribe)
        //         .or(consumer_unsubscribe),
        // )
        // .run(([127, 0, 0, 1], self.addr))
        // .await;
    }

    // pub fn num_partitions(topic: &str) {
    //     topic_to_partitions[topic].len()
    // }
}

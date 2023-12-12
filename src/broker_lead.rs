use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
};

use warp::Filter;

use crate::{
    broker::Broker, consumer_group::ConsumerGroup, ConsumerGroupId, PartitionId, PartitionInfo,
    ServerId, Topic,
};

mod utils;

pub struct BrokerLead {
    addr: ServerId,
    // track which topics
    topic_to_partitions: Arc<Mutex<HashMap<Topic, Vec<PartitionInfo>>>>,
    // how many producers producing a said topic
    topic_to_producer_count: Arc<Mutex<HashMap<Topic, usize>>>,
    // track how many partitions each broker has
    broker_partition_count: Arc<Mutex<HashMap<ServerId, usize>>>,
    // track which consumers are subscribed to which topics
    consumer_group_id_to_groups: Arc<Mutex<HashMap<ConsumerGroupId, ConsumerGroup>>>,
    // number of partitions per topic
    partition_count: usize,
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
            consumer_group_id_to_groups: Arc::new(Mutex::new(HashMap::new())),
            partition_count,
        }
    }

    pub async fn listen(&mut self) {
        println!("BrokerLead listening on port {}", self.addr);

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
                    let mut broker_partition_count = broker_partition_count.lock().unwrap();
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
                        for broker_server_id in broker_server_ids {
                            broker_partition_count
                                .entry(broker_server_id)
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }

                    topic_to_producer_count
                        .entry(topic.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    warp::reply::json(&topic_to_partitions[&topic].clone())
                }
            });

        let producer_remove_topic = warp::delete().and(warp::path!("topics" / String)).map({
            let topic_to_partitions = self.topic_to_partitions.clone();
            let topic_to_producer_count = self.topic_to_producer_count.clone();
            let broker_partition_count = self.broker_partition_count.clone();
            move |topic: String| {
                let mut topic_to_partitions = topic_to_partitions.lock().unwrap();
                let mut topic_to_producer_count = topic_to_producer_count.lock().unwrap();
                let mut broker_partition_count = broker_partition_count.lock().unwrap();
                if !topic_to_partitions.contains_key(&topic) {
                    return warp::reply::json(&"Topic does not exist");
                }

                topic_to_producer_count
                    .entry(topic.clone())
                    .and_modify(|count| *count -= 1)
                    .or_insert(0);

                if topic_to_producer_count[&topic] == 0 {
                    // NOTE: we never remove partitions from brokers
                    let partitions = topic_to_partitions.remove(&topic).unwrap();
                    for partition in partitions {
                        broker_partition_count
                            .entry(partition.server_id)
                            .and_modify(|count| *count -= 1)
                            .or_insert(0);
                    }
                }

                warp::reply::json(&"OK")
            }
        });

        let consumer_subscribe = warp::post()
            .and(warp::path!(ConsumerGroupId / "topics"))
            .and(warp::body::json())
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, topic: Topic| {
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        return warp::reply::json(&"Topic does not exist");
                    }
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(consumer_group_id));
                    consumer_group.subscribe(&topic, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_unsubscribe = warp::delete()
            .and(warp::path!(ConsumerGroupId / "topics" / Topic))
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, topic: Topic| {
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        return warp::reply::json(&"Topic does not exist");
                    }
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(consumer_group_id));
                    consumer_group.unsubscribe(&topic, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_add_group = warp::post()
            .and(warp::path!(ConsumerGroupId / "consumers" / ServerId))
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, server_id: ServerId| {
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(consumer_group_id));
                    consumer_group.add_consumer(server_id, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_remove_group = warp::delete()
            .and(warp::path!(ConsumerGroupId / "consumers" / ServerId))
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, server_id: ServerId| {
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(consumer_group_id));
                    consumer_group.remove_consumer(server_id, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_check_group = warp::get()
            .and(warp::path!(ConsumerGroupId / "consumers" / ServerId))
            .map({
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, server_id: ServerId| {
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new(consumer_group_id));
                    let changes = consumer_group.get_changes(server_id);
                    warp::reply::json(&changes)
                }
            });

        warp::serve(
            producer_add_topic
                .or(producer_remove_topic)
                .or(consumer_subscribe)
                .or(consumer_unsubscribe)
                .or(consumer_check_group)
                .or(consumer_add_group)
                .or(consumer_remove_group),
        )
        .run(([127, 0, 0, 1], self.addr))
        .await;
    }
}

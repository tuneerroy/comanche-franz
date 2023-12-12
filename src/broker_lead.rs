use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use warp::Filter;

use crate::{
    consumer_group::ConsumerGroup,
    listeners::{ConsumerAddGroup, ProducerAddsTopic},
    ConsumerGroupId, PartitionId, PartitionInfo, ServerId, Topic,
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
    pub fn new(addr: u16, broker_ids: Vec<ServerId>, partition_count: usize) -> BrokerLead {
        let mut broker_partition_count = HashMap::new();
        for broker_id in broker_ids {
            broker_partition_count.insert(broker_id, 0);
        }
        BrokerLead {
            addr,
            topic_to_partitions: Arc::new(Mutex::new(HashMap::new())),
            topic_to_producer_count: Arc::new(Mutex::new(HashMap::new())),
            broker_partition_count: Arc::new(Mutex::new(broker_partition_count)),
            consumer_group_id_to_groups: Arc::new(Mutex::new(HashMap::new())),
            partition_count,
        }
    }

    pub async fn listen(&mut self) {
        eprintln!("Kafka cluster listening on {}!", self.addr);

        let producer_add_topic = warp::post()
            .and(warp::path("topics"))
            .and(warp::body::json())
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let topic_to_producer_count = self.topic_to_producer_count.clone();
                let broker_partition_count = self.broker_partition_count.clone();
                let partition_count = self.partition_count;
                move |message: ProducerAddsTopic| {
                    let topic = message.topic;
                    eprintln!("BrokerLead received producer add topic: {:?}", topic);
                    let mut topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut topic_to_producer_count = topic_to_producer_count.lock().unwrap();
                    let mut broker_partition_count = broker_partition_count.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        let broker_server_ids = utils::get_brokers_with_least_partitions(
                            &broker_partition_count,
                            partition_count,
                        );
                        assert_eq!(broker_server_ids.len(), partition_count);
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
                        eprintln!("Partitions created.");
                    } else {
                        eprintln!("Partitions already exists.");
                    }

                    // print partitions
                    for (topic, partitions) in topic_to_partitions.iter() {
                        eprintln!("Topic: {}", topic);
                        for partition in partitions {
                            eprintln!("  Partition: {}", partition.partition_id);
                        }
                    }

                    topic_to_producer_count
                        .entry(topic.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);

                    let reply: Vec<PartitionInfo> = topic_to_partitions[&topic].clone();
                    warp::reply::json(&reply)
                }
            });

        let producer_remove_topic = warp::delete().and(warp::path!("topics" / String)).map({
            let topic_to_partitions = self.topic_to_partitions.clone();
            let topic_to_producer_count = self.topic_to_producer_count.clone();
            let broker_partition_count = self.broker_partition_count.clone();
            move |topic: String| {
                eprintln!("BrokerLead received producer remove topic: {:?}", topic);
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
                    eprintln!("BrokerLead received consumer subscribe: {:?}", topic);
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        return warp::reply::json(&"Topic does not exist");
                    }
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new());
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
                    eprintln!("BrokerLead received consumer unsubscribe: {:?}", topic);
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    if !topic_to_partitions.contains_key(&topic) {
                        return warp::reply::json(&"Topic does not exist");
                    }
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new());
                    consumer_group.unsubscribe(&topic, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_add_group = warp::post()
            .and(warp::path!(ConsumerGroupId / "consumers"))
            .and(warp::body::json())
            .map({
                let topic_to_partitions = self.topic_to_partitions.clone();
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, body: ConsumerAddGroup| {
                    let server_id = body.consumer_id;
                    eprintln!("BrokerLead received consumer add group: {:?}", server_id);
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new());
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
                    eprintln!("BrokerLead received consumer remove group: {:?}", server_id);
                    let topic_to_partitions = topic_to_partitions.lock().unwrap();
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new());
                    consumer_group.remove_consumer(server_id, &topic_to_partitions);
                    warp::reply::json(&"OK")
                }
            });

        let consumer_check_group = warp::get()
            .and(warp::path!(ConsumerGroupId / "consumers" / ServerId))
            .map({
                let consumer_group_id_to_groups = self.consumer_group_id_to_groups.clone();
                move |consumer_group_id: ConsumerGroupId, server_id: ServerId| {
                    eprintln!("BrokerLead received consumer check group: {:?}", server_id);
                    let mut consumer_group_id_to_groups =
                        consumer_group_id_to_groups.lock().unwrap();
                    let consumer_group = consumer_group_id_to_groups
                        .entry(consumer_group_id.clone())
                        .or_insert_with(|| ConsumerGroup::new());
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

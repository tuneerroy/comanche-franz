use std::collections::{HashMap, HashSet};

use crate::{ConsumerInformation, PartitionInfo, ServerId, Topic};

pub struct ConsumerGroup {
    consumer_id_to_info: HashMap<ServerId, ConsumerInformation>,
    topics: HashSet<Topic>,
}

type TopicToPartitionInfo = HashMap<Topic, Vec<PartitionInfo>>;

impl ConsumerGroup {
    pub fn new() -> ConsumerGroup {
        ConsumerGroup {
            consumer_id_to_info: HashMap::new(),
            topics: HashSet::new(),
        }
    }

    fn reorganize_partitions(&mut self, map: &TopicToPartitionInfo) {
        // gather all partitions
        let mut partitions = Vec::new();
        for topic in self.topics.iter() {
            let partition_ids = map.get(topic).unwrap();
            for partition_id in partition_ids {
                partitions.push(partition_id.clone());
            }
        }

        // reset consumer ids
        let mut consumer_ids = Vec::new();
        for (consumer_id, consumer_info) in self.consumer_id_to_info.iter_mut() {
            consumer_info.partition_infos = Vec::new();
            consumer_info.has_received_change = true;
            consumer_ids.push(*consumer_id);
        }

        // assign partitions to consumers
        let mut i = 0;
        for partition in partitions {
            let consumer_id = consumer_ids[i];
            let consumer_info = self.consumer_id_to_info.get_mut(&consumer_id).unwrap();
            consumer_info.partition_infos.push(partition.clone());
            i = (i + 1) % consumer_ids.len();
        }
    }

    pub fn subscribe(&mut self, topic: &Topic, map: &TopicToPartitionInfo) {
        self.topics.insert(topic.clone());

        eprintln!("after add, topics subscribed to: {:?}", self.topics);

        self.reorganize_partitions(map);
    }

    pub fn unsubscribe(&mut self, topic: &Topic, map: &TopicToPartitionInfo) {
        self.topics.remove(topic);

        eprintln!("after remove, topics subscribed to: {:?}", self.topics);

        self.reorganize_partitions(map);
    }

    pub fn add_consumer(&mut self, consumer_id: ServerId, map: &TopicToPartitionInfo) {
        let consumer_info = ConsumerInformation {
            partition_infos: Vec::new(),
            has_received_change: false,
        };
        self.consumer_id_to_info.insert(consumer_id, consumer_info);
        self.reorganize_partitions(map);
    }

    pub fn remove_consumer(&mut self, consumer_id: ServerId, map: &TopicToPartitionInfo) {
        self.consumer_id_to_info.remove(&consumer_id);
        self.reorganize_partitions(map);
    }

    pub fn get_changes(&mut self, consumer_id: ServerId) -> ConsumerInformation {
        let consumer_info = self.consumer_id_to_info.get_mut(&consumer_id).unwrap();
        if !consumer_info.has_received_change {
            return ConsumerInformation {
                partition_infos: Vec::new(),
                has_received_change: false,
            };
        }
        let res = (*consumer_info).clone();
        consumer_info.has_received_change = false;
        res
    }
}

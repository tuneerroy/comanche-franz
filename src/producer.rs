use std::collections::HashMap;

use crate::{
    listeners::{self, ProducerAddsTopic},
    PartitionInfo, ServerId, Topic, Value,
};

mod utils;

pub struct Producer {
    broker_leader_addr: ServerId,
    topic_to_partitions: HashMap<Topic, Vec<PartitionInfo>>,
}

impl Producer {
    pub async fn new(broker_leader_addr: ServerId) -> Producer {
        Producer {
            broker_leader_addr,
            topic_to_partitions: HashMap::new(),
        }
    }

    pub async fn add_topic(&mut self, topic: Topic) -> Result<(), reqwest::Error> {
        if self.topic_to_partitions.contains_key(&topic) {
            eprintln!("Topic already exists.");
            return Ok(());
        }

        let message: ProducerAddsTopic = ProducerAddsTopic {
            topic: topic.clone(),
        };
        let res = reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/topics",
                self.broker_leader_addr
            ))
            .json(&message)
            .send()
            .await?;

        let partitions = res.json().await?;
        self.topic_to_partitions.insert(topic, partitions);

        // print partitions
        for (topic, partition_infos) in self.topic_to_partitions.iter() {
            eprintln!("Topic: {}", topic);
            for partition_info in partition_infos {
                eprintln!(
                    "  Partition: {} {}",
                    partition_info.server_id(),
                    partition_info.partition_id()
                );
            }
        }

        Ok(())
    }

    pub async fn remove_topic(&mut self, topic: Topic) -> Result<(), reqwest::Error> {
        if !self.topic_to_partitions.contains_key(&topic) {
            eprintln!("Topic does not exist.");
            return Ok(());
        }

        reqwest::Client::new()
            .delete(format!(
                "http://127.0.0.1:{}/topics/{}",
                self.broker_leader_addr, topic
            ))
            .send()
            .await?;

        self.topic_to_partitions.remove(&topic);
        Ok(())
    }

    pub async fn send_message(&mut self, topic: Topic, value: Value) -> Result<(), reqwest::Error> {
        if !self.topic_to_partitions.contains_key(&topic) {
            eprintln!("Topic does not exist.");
            return Ok(());
        }

        let partition_infos = self.topic_to_partitions.get(&topic).unwrap();
        let value_to_be_hashed = format!("{}{}", topic, value);
        let i = utils::hash_value_to_range(&value_to_be_hashed, partition_infos.len());
        let partition_info = &partition_infos[i];

        let message = listeners::ProducerSendsMessage { value };

        reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/{}/messages",
                partition_info.server_id(),
                partition_info.partition_id(),
            ))
            .json(&message)
            .send()
            .await?;

        Ok(())
    }
}

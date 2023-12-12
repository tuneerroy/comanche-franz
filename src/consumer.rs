use std::collections::HashMap;

use crate::{
    listeners::{ConsumerAddGroup, ConsumerSubscribes, ConsumerRequestsMessage},
    ConsumerGroupId, ConsumerInformation, ServerId, Topic, Value, PartitionInfoWithOffset, PartitionInfo,
};

pub struct Consumer {
    addr: ServerId,
    broker_leader_addr: ServerId,
    partitions: Vec<PartitionInfoWithOffset>,
    consumer_group_id: Option<ConsumerGroupId>,
}

impl Consumer {
    pub fn new(addr: ServerId, broker_leader_addr: ServerId) -> Consumer {
        Consumer {
            addr,
            broker_leader_addr,
            partitions: Vec::new(),
            consumer_group_id: None,
        }
    }

    pub async fn subscribe(&mut self, topic: Topic) -> Result<(), reqwest::Error> {
        if self.consumer_group_id.is_none() {
            eprintln!("Not in consumer group.");
            return Ok(());
        }

        let message = ConsumerSubscribes { topic };
        reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/groups/{}/topics",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap()
            ))
            .json(&message)
            .send()
            .await?;

        Ok(())
    }

    pub async fn unsubscribe(&mut self, topic: Topic) -> Result<(), reqwest::Error> {
        if self.consumer_group_id.is_none() {
            eprintln!("Not in consumer group.");
            return Ok(());
        }

        reqwest::Client::new()
            .delete(format!(
                "http://127.0.0.1:{}/groups/{}/topics/{}",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap(),
                topic
            ))
            .send()
            .await?;

        Ok(())
    }

    pub async fn join_consumer_group(
        &mut self,
        consumer_group_id: ConsumerGroupId,
    ) -> Result<(), reqwest::Error> {
        if self.consumer_group_id.is_some() {
            eprintln!("Already in consumer group.");
            return Ok(());
        }

        let message = ConsumerAddGroup {
            consumer_id: self.addr,
        };

        reqwest::Client::new()
            .post(format!(
                "http://127.0.0.1:{}/groups/{}/consumers",
                self.broker_leader_addr, consumer_group_id
            ))
            .json(&message)
            .send()
            .await?;

        self.consumer_group_id = Some(consumer_group_id);
        Ok(())
    }

    pub async fn leave_consumer_group(&mut self) -> Result<(), reqwest::Error> {
        if self.consumer_group_id.is_none() {
            eprintln!("Not in consumer group.");
            return Ok(());
        }

        reqwest::Client::new()
            .delete(format!(
                "http://127.0.0.1:{}/groups/{}/consumers/{}",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap(),
                self.addr
            ))
            .send()
            .await?;

        self.consumer_group_id = None;
        Ok(())
    }

    pub async fn get_offset(&self, partition: &PartitionInfo) -> Result<usize, reqwest::Error> {
        let res = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/{}/offset", partition.server_id(), partition.partition_id()))
            .send()
            .await?;

        Ok(res.json::<usize>().await?)
    }

    pub async fn poll(&mut self) -> Result<Vec<Value>, reqwest::Error> {
        if self.consumer_group_id.is_none() {
            eprintln!("Not in consumer group.");
            return Ok(Vec::new());
        }

        // first check if any changes to partitions
        let res = reqwest::Client::new()
            .get(format!(
                "http://127.0.0.1:{}/groups/{}/consumers/{}",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap(),
                self.addr
            ))
            .send()
            .await?;

        let consumer_info = res.json::<ConsumerInformation>().await?;
        if consumer_info.has_received_change {
            eprintln!("Received change: {:?}", consumer_info);
            // get current offsets mapping from partition id to offset
            let mut partition_id_to_offset = HashMap::new();
            for partition_info_with_offset in self.partitions.iter() {
                let id = partition_info_with_offset.partition_info.partition_id();
                let offset = partition_info_with_offset.offset();
                partition_id_to_offset.insert(id, offset);
            }

            let partition_infos = consumer_info.partition_infos;
            let mut new_partitions = Vec::new();
            for partition_info in partition_infos {
                // check if we already have it
                if let Some(offset) = partition_id_to_offset.get(&partition_info.partition_id()) {
                    new_partitions.push(PartitionInfoWithOffset::new(partition_info.clone(), *offset));
                } else {
                    // if not, get the offset
                    let offset = self.get_offset(&partition_info).await?;
                    new_partitions.push(PartitionInfoWithOffset::new(partition_info, offset));
                }
            }
            self.partitions = new_partitions;
        }

        let mut all_values = Vec::new();
        for partition_info_with_offset in self.partitions.iter() {
            let msg: ConsumerRequestsMessage = ConsumerRequestsMessage { offset: partition_info_with_offset.offset() };
            let partition_info = &partition_info_with_offset.partition_info;
            let res = reqwest::Client::new()
                .get(format!(
                    "http://127.0.0.1:{}/{}/messages",
                    partition_info.server_id(),
                    partition_info.partition_id()
                ))
                .json(&msg)
                .send()
                .await?;
            all_values.push(res.json::<Value>().await?);
        }

        Ok(all_values)
    }
}

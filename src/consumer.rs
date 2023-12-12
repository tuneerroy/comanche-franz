use serde::{Deserialize, Serialize};

use crate::{
    listeners::{ConsumerAddGroup, ConsumerSubscribes},
    ConsumerGroupId, ConsumerInformation, PartitionInfo, ServerId, Topic, Value,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerRequestsMessage {
    pub offset: usize,
    pub size: usize,
}

pub struct Consumer {
    addr: ServerId,
    broker_leader_addr: ServerId,
    partitions: Vec<PartitionInfo>,
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
                "http://localhost:{}/{}/topics",
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
                "http://localhost:{}/{}/topics/{}",
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
                "http://localhost:{}/{}/consumers",
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
                "http://localhost:{}/{}/consumers/{}",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap(),
                self.addr
            ))
            .send()
            .await?;

        self.consumer_group_id = None;
        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Vec<Value>, reqwest::Error> {
        if self.consumer_group_id.is_none() {
            eprintln!("Not in consumer group.");
            return Ok(Vec::new());
        }

        // first check if any changes to partitions
        let res = reqwest::Client::new()
            .get(format!(
                "http://localhost:{}/{}/consumers/{}",
                self.broker_leader_addr,
                self.consumer_group_id.as_ref().unwrap(),
                self.addr
            ))
            .send()
            .await?;

        let consumer_info = res.json::<ConsumerInformation>().await?;
        if consumer_info.has_received_change {
            eprintln!("Received change: {:?}", consumer_info);
            self.partitions = consumer_info.partition_infos;
        }

        let mut all_values = Vec::new();
        for partition_info in self.partitions.iter() {
            let msg: ConsumerRequestsMessage = ConsumerRequestsMessage { offset: 0, size: 1 };
            let res = reqwest::Client::new()
                .get(format!(
                    "http://localhost:{}/{}/messages",
                    partition_info.server_id(),
                    partition_info.partition_id(),
                ))
                .json(&msg)
                .send()
                .await?;

            let values = res.json::<Vec<Value>>().await?;
            all_values.extend(values);
        }

        Ok(all_values)
    }
}

use crate::{
    listeners::{ConsumerAddGroup, ConsumerRequestsMessage, ConsumerSubscribes},
    ConsumerGroupId, ConsumerInformation, ConsumerResponse, PartitionInfo,
    ServerId, Topic, Value,
};

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
            .get(format!(
                "http://127.0.0.1:{}/{}/offset",
                partition.server_id(),
                partition.partition_id()
            ))
            .send()
            .await?;

        res.json::<usize>().await
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
            self.partitions = consumer_info.partition_infos;
            eprintln!("New partitions: {:?}", self.partitions);
        }

        let mut all_values = Vec::new();
        for partition_info in self.partitions.iter_mut() {
            let msg: ConsumerRequestsMessage = ConsumerRequestsMessage {
                consumer_group_id: self.consumer_group_id.as_ref().unwrap().clone(),
            };
            let res = reqwest::Client::new()
                .get(format!(
                    "http://127.0.0.1:{}/{}/messages",
                    partition_info.server_id(),
                    partition_info.partition_id()
                ))
                .json(&msg)
                .send()
                .await?;
            let res: ConsumerResponse = res.json::<ConsumerResponse>().await?;
            all_values.push(res.value);
        }

        Ok(all_values)
    }
}

use std::fmt::Formatter;

use serde::{Deserialize, Serialize};

pub type ServerId = u16;
pub type Topic = String;
pub type Value = String;

mod broker {
    mod listeners;
    mod requests;
    mod utils;
}

mod broker_lead {
    mod listeners;
    mod requests;
}

mod partition_stream;

mod consumer {
    mod listeners;
    mod requests;
}

mod producer {
    mod listeners;
    mod requests;
    mod utils;
}

mod utils;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct PartitionId {
    topic: Topic,
    partition_num: usize,
}

impl PartitionId {
    pub fn new(topic: Topic, partition_num: usize) -> PartitionId {
        PartitionId {
            topic,
            partition_num,
        }
    }

    pub fn from_str(partition_id: &str) -> PartitionId {
        let mut split = partition_id.split('-');
        let topic = split.next().unwrap().to_string();
        let partition_num = split.next().unwrap().parse::<usize>().unwrap();
        PartitionId {
            topic,
            partition_num,
        }
    }
}

impl std::fmt::Display for PartitionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition_num)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    partition_id: PartitionId,
    server_id: ServerId,
}

impl PartitionInfo {
    pub fn new(partition_id: PartitionId, server_id: ServerId) -> PartitionInfo {
        PartitionInfo {
            partition_id,
            server_id,
        }
    }

    pub fn partition_id(&self) -> &PartitionId {
        &self.partition_id
    }

    pub fn server_id(&self) -> &ServerId {
        &self.server_id
    }
}

use std::fmt::Formatter;

use serde::{Deserialize, Serialize};

pub type ServerId = u16;
pub type Topic = String;
pub type Value = String;

pub type ConsumerGroupId = String;

pub mod broker;
pub mod broker_lead;
pub mod consumer;
pub mod consumer_group;
pub mod producer;

// TODO: most of this should be in a utils file, not here
// really should just be importigna and re-exposing certain things
mod listeners {
/****************** FOR THE BROKER LISTENERS ******/
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct ProducerSendsMessage {
        pub value: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct ConsumerRequestsMessage {
        pub offset: usize,
        pub size: usize,
    }

    /****************** FOR THE BROKER LEADER LISTENERS ******/

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct ProducerAddsTopic {
        topic: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct ConsumerSubscribes {
        topic: String,
    }
}
/****************** FOR OTHER STUFF ******/

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerInformation {
    pub partition_infos: Vec<PartitionInfo>,
    pub has_received_change: bool,
}

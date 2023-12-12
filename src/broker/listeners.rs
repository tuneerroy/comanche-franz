use serde::{Deserialize, Serialize};

// TODO: these should really be shared across all the different services

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerSendsMessage {
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerRequestsMessage {
    pub offset: usize,
    pub size: usize,
}

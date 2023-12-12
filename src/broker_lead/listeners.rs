use serde::{Deserialize, Serialize};

// TODO: these should really be shared across all the different services

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerAddsTopic {
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerSubscribes {
    topic: String,
}

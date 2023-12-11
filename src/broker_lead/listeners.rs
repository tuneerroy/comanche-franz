use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerAddsTopic {
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerSubscribes {
    topic: String,
}

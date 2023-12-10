use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerSendsMessage {
    pub topic: String,
    pub partition: u16,
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerRequestsMessage {
    pub topic: String,
    pub partition: u16,
    pub offset: usize,
    // TODO: add a size field for how many bytes to read
}

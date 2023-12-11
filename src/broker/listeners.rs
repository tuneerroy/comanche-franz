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

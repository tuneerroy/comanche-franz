use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use warp::Filter;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerAddsTopic {
    producer_id: u16,
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProducerRemovesTopic {
    producer_id: u16,
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerSubscribes {
    consumer_id: u16,
    topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ConsumerUnsubscribes {
    consumer_id: u16,
    topic: String,
}

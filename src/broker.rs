use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use warp::Filter;

use crate::{PartitionId, ServerId};

use self::utils::Partition;

mod listeners;
mod utils;

pub struct Broker {
    addr: ServerId,
    partitions: Arc<Mutex<HashMap<PartitionId, Partition>>>,
}

impl Broker {
    pub fn new(addr: u16) -> Broker {
        Broker {
            addr,
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn listen(&mut self) {
        let producer_sends_message = warp::post()
            .and(warp::path!(String / "messages"))
            .and(warp::body::json())
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String, message: listeners::ProducerSendsMessage| {
                    let partition_id = PartitionId::from_str(&partition_id);
                    let mut partitions = partitions.lock().unwrap();
                    let partition = partitions
                        .entry(partition_id.clone())
                        .or_insert_with(|| Partition::new(partition_id.to_string().clone()));
                    partition.append(&message.value);
                    println!("Broker received producer message: {:?}", message);
                    warp::reply::reply()
                }
            });

        let consumer_requests_message = warp::post()
            .and(warp::path!(String / "messages"))
            .and(warp::body::json())
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String, message: listeners::ConsumerRequestsMessage| {
                    let partition_id = PartitionId::from_str(&partition_id);
                    let mut partitions = partitions.lock().unwrap();
                    let partition = partitions
                        .entry(partition_id.clone())
                        .or_insert_with(|| Partition::new(partition_id.to_string().clone()));
                    let contents = partition.read(message.offset);
                    println!("Broker received consumer request: {:?}", message);
                    warp::reply::json(&contents)
                }
            });

        warp::serve(producer_sends_message.or(consumer_requests_message))
            .run(([127, 0, 0, 1], self.addr))
            .await;
    }
}

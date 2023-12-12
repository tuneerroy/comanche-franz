use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use warp::Filter;

use crate::{listeners, ConsumerResponse, PartitionId, ServerId, ConsumerGroupId};

use self::utils::Partition;

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
        eprintln!("Broker listening on port {}", self.addr);

        let producer_sends_message = warp::post()
            .and(warp::path!(String / "messages"))
            .and(warp::body::json())
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String, message: listeners::ProducerSendsMessage| {
                    eprintln!("Broker received producer message: {:?}", message);
                    let partition_id = PartitionId::from_str(&partition_id);
                    let mut partitions = partitions.lock().unwrap();
                    let partition = partitions
                        .entry(partition_id.clone())
                        .or_insert_with(|| Partition::new(partition_id.to_string().clone()));
                    partition.append(&message.value);
                    eprintln!("Broker received producer message: {:?}", message);
                    warp::reply::reply()
                }
            });

        let consumer_requests_message = warp::get()
            .and(warp::path!(String / "messages"))
            .and(warp::body::json())
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String, message: listeners::ConsumerRequestsMessage| {
                    eprintln!("Broker received consumer request: {:?}", message);
                    let partition_id = PartitionId::from_str(&partition_id);
                    let mut partitions = partitions.lock().unwrap();
                    let partition = partitions
                        .entry(partition_id.clone())
                        .or_insert_with(|| Partition::new(partition_id.to_string().clone()));
                    eprintln!("Broker received consumer request: {:?}", message);
                    let content = partition.read(message.consumer_group_id);
                    let response: ConsumerResponse = ConsumerResponse {
                        value: content,
                    };
                    warp::reply::json(&response)
                }
            });

        let consumer_group_offset = warp::post()
            .and(warp::path!(String / "consumers" / ConsumerGroupId))
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String, consumer_group_id: ConsumerGroupId| {
                    eprintln!("Broker received consumer group offset: {:?}", consumer_group_id);
                    let partition_id = PartitionId::from_str(&partition_id);
                    let mut partitions = partitions.lock().unwrap();
                    let partition = partitions
                        .entry(partition_id.clone())
                        .or_insert_with(|| Partition::new(partition_id.to_string().clone()));
                    eprintln!("Broker received consumer group offset: {:?}", consumer_group_id);
                    partition.initialize_offset(consumer_group_id);
                    warp::reply::reply()
                }
            });

        warp::serve(
            producer_sends_message
                .or(consumer_requests_message)
                .or(consumer_group_offset),
        )
        .run(([127, 0, 0, 1], self.addr))
        .await;
    }



}

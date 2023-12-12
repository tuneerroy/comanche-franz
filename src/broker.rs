use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use warp::Filter;

use crate::{listeners, PartitionId, ServerId, ConsumerResponse};

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
                    let content = partition.read(message.offset);
                    let response: ConsumerResponse = ConsumerResponse {
                        value: content,
                        new_offset: partition.get_offset(),
                    };
                    eprintln!("Broker received consumer request: {:?}", message);
                    warp::reply::json(&response)
                }
            });

        let consumer_get_offset = warp::get()
            .and(warp::path!(String / "offset"))
            .map({
                let partitions = self.partitions.clone();
                move |partition_id: String| {
                    eprintln!("Broker received consumer request for offset");
                    let partition_id = PartitionId::from_str(&partition_id);
                    let partitions = partitions.lock().unwrap();
                    let partition = partitions.get(&partition_id).unwrap();
                    let offset = partition.get_offset();
                    eprintln!("Broker received consumer request for offset");
                    warp::reply::json(&offset)
                }
            });

        warp::serve(producer_sends_message.or(consumer_requests_message).or(consumer_get_offset))
            .run(([127, 0, 0, 1], self.addr))
            .await;
    }
}

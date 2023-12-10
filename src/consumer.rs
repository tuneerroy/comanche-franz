use std::{thread, sync::{Mutex, Arc}};
use serde::{Deserialize, Serialize};

// use crate::partition_stream::PartitionStream;
// TEMP BECAUSE mod wasn't working
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PartitionStream {
    topic: String,
    server: u16,
    offset: usize,
}

mod listeners;
use listeners::UpdateOffset;

use warp::Filter;
pub struct Consumer {
    // id: ServerId,
    id: u16,
    topics: Vec<String>,
    streams: Arc<Mutex<Vec<PartitionStream>>>,
    //brokers: Vec<ServerId>,
}

impl Consumer {
    pub fn get_topics(&self) -> &Vec<String> {
        return &self.topics;
    }

    // Turn into a route
    pub fn split_off_partitions(&mut self, n: usize) -> Vec<PartitionStream> {
        let mut streams = self.streams.lock().unwrap();
        let mut partitions = Vec::new();
        for _ in 0..n {
            partitions.push(streams.pop().unwrap());
        }
        return partitions;
    }

    // Turn into a route
    pub fn push_stream(&mut self, stream: PartitionStream) {
        self.streams.lock().unwrap().push(stream);
    }

    pub async fn listen(&mut self) {
        let streams_clone = self.streams.clone();
        let update_offset = warp::post()
            .and(warp::path("partitions"))
            .and(warp::body::json())
            .map(move |body: UpdateOffset| {
                let mut streams = streams_clone.lock().unwrap();
                for stream in streams.iter_mut() {
                    if stream.server == body.partition { // ignoring the topic?
                        stream.offset = body.offset;
                    }
                }
                warp::reply::json(&"Offset updated")
            });

        warp::serve(update_offset)
            .run(([127, 0, 0, 1], self.id))
            .await;
    }

    // pub async fn new(id: u16, broker: ServerId) -> Consumer {
    pub async fn new(id: u16) -> Consumer {
        // want to make thread to call listen?
        Consumer {
            id,
            topics: Vec::new(),
            streams: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

use crate::partition_stream::PartitionStream;
use std::thread;

use warp::Filter;
pub struct Consumer {
    // id: ServerId,
    id: u16,
    topics: Vec<String>,
    streams: Vec<PartitionStream>,
    //brokers: Vec<ServerId>,
}

impl Consumer {
    pub fn get_topics(&self) -> &Vec<String> {
        return &self.topics;
    }

    // Turn into a route
    pub fn split_off_partitions(&mut self, n: usize) -> Vec<PartitionStream> {
        return self.streams.split_off(n);
    }

    // Turn into a route
    pub fn push_stream(&mut self, stream: PartitionStream) {
        self.streams.push(stream);
    }

    pub async fn listen(&mut self) {
        let update_offset = warp::post()
            .and(warp::path("partitions"))
            .and(warp::body::json())
            .map(|body: UpdateOffset| {
                let mut stream = self.streams[body.partition as usize].clone();
                stream.set_offset(body.offset);
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
            streams: Vec::new(),
        }
    }
}

use std::{thread, collections::HashSet};
use comanche_franz::ServerId;
use warp::Filter;

pub struct Producer {
    id: ServerId,
    brokers: Vec<ServerId>,
    topics: HashSet<String>,
    current_index: usize,
}

pub fn return_heartbeat() -> impl warp::Reply {
    warp::reply::json(&"alive")
}

impl Producer {
    pub async fn new(id: ServerId, broker: ServerId) -> Producer {
        let mut all_brokers = Vec::new();
        all_brokers.push(id);

        thread::spawn(move || async move {
            let get_heartbeat = warp::get()
                .and(warp::path("heartbeat"))
                .map(return_heartbeat); 

            warp::serve(get_heartbeat)
                .run(([127, 0, 0, 1], id))
                .await;
        });

        Producer {
            id,
            brokers: all_brokers,
            current_index: 0,
            topics: HashSet::new(),
        }
    }

    pub async fn add_topic(&mut self, topic: String) -> () {
        if self.topics.contains(&topic) {
            return;
        }
        self.topics.insert(topic.clone());

        reqwest::Client::new().post({
            let str = format!("localhost:{}/add_topic", self.brokers[self.current_index]);
            self.current_index = (self.current_index + 1) % self.brokers.len();
            str
    })
            .json(&topic)
            .send()
            .await
            .expect("Failed to send request to broker");
    }
}
use std::{collections::{HashMap, HashSet}, thread, future::Future, process::Output};

use comanche_franz::ServerId;
use tokio::{io::AsyncReadExt, net::TcpListener};

pub struct Broker {
    id: ServerId,
    is_leader: bool,
    all_brokers: HashSet<ServerId>,
    topics_to_consumers: HashMap<String, HashSet<ServerId>>,
    producers_to_topics: HashMap<ServerId, HashSet<String>>,
}

impl Broker {
    pub async fn new(id: ServerId, other: Option<ServerId>) -> Broker {
        let all_brokers: HashSet<ServerId> = HashSet::new();
        thread::spawn(move || async move {
            let listener = TcpListener::bind(format!("localhost:{}", id))
                .await
                .expect("Failed to bind to port");
            loop {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .expect("Failed to accept connection");
                let mut buffer = [0; 1024];
                socket
                    .read(&mut buffer)
                    .await
                    .expect("Failed to read from socket");
            };
        });

        Broker {
            id,
            is_leader: all_brokers.is_empty(),
            all_brokers: HashSet::new(),
            topics_to_consumers: HashMap::new(),
            producers_to_topics: HashMap::new(),
        }
    }

    pub fn get_id(&self) -> ServerId {
        self.id
    }

    pub async fn subscribe(&mut self, topic: String, consumer: ServerId) -> () {
        if !self.topics_to_consumers.contains_key(&topic) {
            self.topics_to_consumers.insert(topic.clone(), HashSet::new());
        }
        self.topics_to_consumers
            .get_mut(&topic)
            .unwrap()
            .insert(consumer);
    }
}

async fn get_all_brokers(id: ServerId) -> Result<HashSet<ServerId>, reqwest::Error> {
    // let response = reqwest::get(format!("localhost:{id}/brokers")).await?;
    // let brokers: Vec<ServerId> = response.json().await?;
    // Ok(brokers)
    Ok(HashSet::new())
}

async fn get_topics_to_consumers(
    id: ServerId,
) -> Result<HashMap<String, HashSet<ServerId>>, reqwest::Error> {
    // let response = reqwest::get(format!("localhost:{id}/topics")).await?;
    // let topics_to_consumers: HashMap<String, Vec<ServerId>> = response.json().await?;
    // Ok(topics_to_consumers)
    Ok(HashMap::new())
}

use std::{collections::HashMap, thread};

use comanche_franz::{ServerId, Service};
use tokio::{net::TcpListener, io::AsyncReadExt};

pub struct Broker {
    id: ServerId,
    is_leader: bool,
    all_brokers: Vec<ServerId>,
    topics_to_consumers: HashMap<String, Vec<ServerId>>,
}

impl Broker {
    pub async fn from_terminal(terminal_args: Vec<String>) -> Result<Broker, &'static str> {
        if terminal_args.len() < 2 {
            return Err("Need argument for server port")
        }
        let id: ServerId = terminal_args[1].parse::<ServerId>().map_err(|_| "Failed to parse server port")?;
        if terminal_args.len() < 3 {
            return Ok(Broker::new(id, None).await)
        }
        let other_id: ServerId = terminal_args[2].parse::<ServerId>().map_err(|_| "Failed to parse other broker server port")?;
        Ok(Broker::new(id, Some(other_id)).await)
    }

    pub async fn new(id: ServerId, other: Option<ServerId>) -> Broker {
        let mut all_brokers: Vec<ServerId>;
        let topics_to_consumers: HashMap<String, Vec<ServerId>>;
        match other {
            Some (other_id) => {
                all_brokers = get_all_brokers(other_id).await.expect("Failed to get all brokers");
                topics_to_consumers = get_topics_to_consumers(other_id).await.expect("Failed to get topics to consumers");
            },
            None => {
                all_brokers = Vec::new();
                topics_to_consumers = HashMap::new();
            },
        }
        all_brokers.push(id);

        thread::spawn(move || async move {
            let listener = TcpListener::bind(format!("localhost:{}", id)).await.expect("Failed to bind to port");
            loop {
                let (mut socket, _) = listener.accept().await.expect("Failed to accept connection");
                let mut buffer = [0; 1024];
                socket.read(&mut buffer).await.expect("Failed to read from socket");
            }
        });

        Broker {
            id,
            is_leader: all_brokers.is_empty(),
            all_brokers,
            topics_to_consumers,
        }
    }

}

impl Service for Broker {
    fn serve_command(&mut self, command: String) -> () {
        !unimplemented!()
    }
}

async fn get_all_brokers(id: ServerId) -> Result<Vec<ServerId>, reqwest::Error> {
    let response = reqwest::get(format!("localhost:{id}/brokers")).await?;
    let brokers: Vec<ServerId> = response.json().await?;
    Ok(brokers)
}

async fn get_topics_to_consumers(id: ServerId) -> Result<HashMap<String, Vec<ServerId>>, reqwest::Error> {
    let response = reqwest::get(format!("localhost:{id}/topics")).await?;
    let topics_to_consumers: HashMap<String, Vec<ServerId>> = response.json().await?;
    Ok(topics_to_consumers)
}
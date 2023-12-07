use std::thread;

use comanche_franz::ServerId;
use tokio::{net::TcpListener, io::AsyncReadExt};

pub struct Consumer {
    id: ServerId,
    topics: Vec<String>,
    brokers: Vec<ServerId>,
}

impl Consumer {
    pub async fn new(id: ServerId, broker: ServerId) -> Consumer {
        let mut all_brokers: Vec<ServerId> = Vec::new();
        all_brokers.push(id);

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

        Consumer {
            id,
            brokers: all_brokers,
            topics: Vec::new(),
        }
    }
}

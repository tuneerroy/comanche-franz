use std::net::TcpListener;

use comanche_franz::Service;

pub struct Producer {
    handle: std::thread::JoinHandle<()>,
    topics: Vec<String>,
}

impl Producer {
    pub fn new(listener: TcpListener) -> Producer {
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let _stream = stream.expect("Failed to accept server stream result");
            }
        });
        Producer {
            handle,
            topics: Vec::new(),
        }
    }
}

impl Service for Producer { }
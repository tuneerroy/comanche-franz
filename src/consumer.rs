use std::net::TcpListener;

use comanche_franz::Service;

pub struct Consumer {
    handle: std::thread::JoinHandle<()>,
    topics: Vec<String>,
}

impl Consumer {
    pub fn new(listener: TcpListener) -> Consumer {
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let _stream = stream.expect("Failed to accept server stream result");
            }
        });
        Consumer {
            handle,
            topics: Vec::new(),
        }
    }

}

impl Service for Consumer { }
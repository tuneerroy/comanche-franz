use std::net::TcpListener;

use comanche_franz::Service;

pub struct Broker {
    handle: std::thread::JoinHandle<()>,
    consumer_addrs: Vec<String>,
}

impl Broker {
    pub fn new(listener: TcpListener) -> Broker {
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                let _stream = stream.expect("Failed to accept server stream result");
            }
        });
        Broker {
            handle,
            consumer_addrs: Vec::new(),
        }
    }

}

impl Service for Broker { }
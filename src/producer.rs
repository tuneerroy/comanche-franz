use std::net::TcpListener;

use comanche_franz::{ServerId, Service};

pub struct Producer {
    id: ServerId,
    brokers: Vec<ServerId>,
    current_index: usize,
}

impl Producer {
    pub async fn from_terminal(terminal_args: Vec<String>) -> Result<Producer, &'static str> {
        !unimplemented!()
    }

    pub fn new(id: ServerId, broker: ServerId) -> Producer {
        // TODO: make request to broker to get list of brokers
        !unimplemented!()
    }
}

impl Service for Producer {
    fn serve_command(&mut self, command: String) -> () {
        !unimplemented!()
    }
}

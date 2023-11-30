use std::net::TcpListener;

use comanche_franz::{ServerId, Service};

pub struct Consumer {
    id: ServerId,
    topics: Vec<String>,
    brokers: Vec<ServerId>,
}

impl Consumer {
    pub async fn from_terminal(terminal_args: Vec<String>) -> Result<Consumer, &'static str> {
        !unimplemented!()
    }

    pub fn new(id: ServerId, broker: ServerId) -> Consumer {
        // TODO: make request to broker to get list of topics & get
        // list of brokers in case this consumer wants to change topics
        !unimplemented!()
    }

}

impl Service for Consumer {
    fn serve_command(&mut self, command: String) -> () {
        !unimplemented!()
    }
}
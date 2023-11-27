use std::thread;
use std::time::Duration;

use comanche_franz::Record;

mod server;
mod producer;
mod consumer;

const ADDRESS: &str = "localhost:8080";

fn setup() {
    thread::spawn(|| {
        server::run_server(ADDRESS);
    });
}

fn create_basic_producer() -> producer::Producer<String, String> {
    producer::Producer::new(ADDRESS.to_string())
}

fn main() {
    setup();

    let mut producer = create_basic_producer();
    producer.send(Record::new("topic".to_string(), "key".to_string(), "value".to_string()));

    loop {
        println!("Comanche Kafka is active!");
        thread::sleep(Duration::from_secs(1));
    }
}

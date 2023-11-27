use std::thread;
use std::time::Duration;
mod server;

fn setup() {
    thread::spawn(|| {
        server::run_server();
    });
}

fn main() {
    setup();

    loop {
        println!("Comanche Kafka is active!");
        thread::sleep(Duration::from_secs(1));
    }
}

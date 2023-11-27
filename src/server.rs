use std::net::{TcpListener, TcpStream};

fn handle_connection(_stream: TcpStream) {
    // TODO: handle receiving message from producer
    // TODO: get the topic
    // TODO: forward message to respective consumer
}

pub fn run_server() {
    println!("Server is running!");
    let server = TcpListener::bind("localhost:8080").expect("Could not bind");

    for stream in server.incoming() {
        let stream = stream.expect("Failed to accept server stream result");
        handle_connection(stream);
    }

    println!("Server has shut down!");
}
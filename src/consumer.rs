// use serde::de::DeserializeOwned;

// pub struct Consumer<K, V>
// where
//     K: DeserializeOwned,
//     V: DeserializeOwned,
// {
//     topic: String,
//     _key: std::marker::PhantomData<K>,
//     _value: std::marker::PhantomData<V>,
// }

// impl<K, V> Consumer<K, V>
// where
//     K: DeserializeOwned,
//     V: DeserializeOwned,
// {
//     pub fn new(topic: String) -> Consumer<K, V> {
//         Consumer {
//             topic,
//             _key: std::marker::PhantomData,
//             _value: std::marker::PhantomData,
//         }
//     }

//     pub fn consume(&mut self) -> Record<K, V>
//     where
//         K: DeserializeOwned + std::fmt::Debug,
//         V: DeserializeOwned + std::fmt::Debug,
//     {
//         println!("Consuming record from topic: {}", self.topic);

//         // let mut stream = std::net::TcpStream::connect("localhost:8080").expect("Failed to connect to server");
//         // let mut buffer = [0; 1024];
//         // stream.read(&mut buffer).expect("Failed to read from server");

//         // let record: Record<K, V> = serde_json::from_str(std::str::from_utf8(&buffer).expect("Failed to convert buffer to string")).expect("Failed to deserialize record");

//         println!("Record consumed: {:?}", record);

//         record
//     }

//     // pub fn subscribe(&mut self, 
//     //     println!("Subscribing to topic: {}", self.topic);
//     // }

//     // pub fn unsubscribe(&mut self) {
//     //     
// }

use std::net::TcpListener;

pub fn run_service(listener: TcpListener) {
    println!("Consumer is running!");
    for stream in listener.incoming() {
        let _stream = stream.expect("Failed to accept server stream result");
    }
    println!("Consumer has shut down!");
}
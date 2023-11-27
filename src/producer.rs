use std::io::Write;

use comanche_franz::Record;
use serde::Serialize;

pub struct Producer<K, V> 
where
    K: Serialize,
    V: Serialize,
{
    target_address: String,
    _key: std::marker::PhantomData<K>,
    _value: std::marker::PhantomData<V>,
}

impl<K, V> Producer<K, V>
where
    K: Serialize,
    V: Serialize,
{
    pub fn new(address: String) -> Producer<K, V> {
        Producer {
            target_address: address,
            _key: std::marker::PhantomData,
            _value: std::marker::PhantomData,
        }
    }

    pub fn send(&mut self, record: Record<K, V>)
    where
        K: Serialize + std::fmt::Debug,
        V: Serialize + std::fmt::Debug,
    {
        println!("Sending record to server: {:?}", record);

        let mut stream = std::net::TcpStream::connect(&self.target_address).expect("Failed to connect to server");
        let serialized_record = serde_json::to_string(&record.to_string()).expect("Failed to serialize record");
        stream.write(serialized_record.as_bytes()).expect("Failed to write to server");
        stream.flush().expect("Failed to flush stream");

        println!("Record sent!")
    }
}
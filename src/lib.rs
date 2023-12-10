mod broker {
    mod listeners;
    mod requests;
    mod utils;
}

mod broker_lead {
    mod listeners;
    mod requests;
}

mod partition_stream;

mod consumer {
    mod listeners;
    mod requests;
}

mod producer {
    mod listeners;
    mod requests;
}

// mod consumer_group {
//     mod listeners;
//     mod requests;
// }

mod utils;

pub type ServerId = u16;

pub type Topic = String;



// TODO: set up serialization and deserialization later
// use serde::{Serialize, ser::SerializeStruct};

// #[derive(Debug)]
// pub struct Record<K, V>
// where
//     K: Serialize,
//     V: Serialize,
// {
//     topic: String,
//     key: K,
//     value: V,
// }

// impl<K, V> Serialize for Record<K, V>
// where
//     K: Serialize,
//     V: Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer
//     {
//         let mut state = serializer.serialize_struct("Record", 3)?;
//         state.serialize_field("topic", &self.topic)?;
//         state.serialize_field("key", &self.key)?;
//         state.serialize_field("value", &self.value)?;
//         state.end()
//     }
// }

// impl<K, V> Record<K, V>
// where
//     K: Serialize,
//     V: Serialize,
// {
//     pub fn new(topic: String, key: K, value: V) -> Record<K, V> {
//         Record {
//             topic,
//             key,
//             value,
//         }
//     }

//     pub fn to_string(&self) -> String
//     where
//         K: Serialize + std::fmt::Debug,
//         V: Serialize + std::fmt::Debug,
//     {
//         serde_json::to_string(self).expect("Failed to serialize record")
//     }
// }

// pub trait Service {
//     fn serve_command(&mut self, command: String);
// }

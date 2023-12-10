use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use warp::Filter;

mod listeners;
mod utils;

pub struct Broker {
    addr: u16,
    topic_to_offset: Arc<Mutex<HashMap<String, usize>>>,
}

impl Broker {
    pub async fn new(addr: u16) -> Broker {
        let topic_to_offset: HashMap<String, usize> = HashMap::new();

        Broker {
            addr,
            topic_to_offset: Arc::new(Mutex::new(topic_to_offset)),
        }
    }

    pub async fn listen(&mut self) {
        // let consumer_requests_message = warp::post()
        //     .and(warp::path("consumer/messages"))
        //     .and(warp::body::json())
        //     .and_then(|message: listeners::ConsumerRequestsMessage| async move {
        //         let filename = format!("{}-{}.log", message.topic, message.partition);
        //         let contents = utils::read_from_file(&filename, message.offset).await;
        //         println!("Broker received consumer request: {:?}", message);
        //         Ok::<_, warp::Rejection>(warp::reply::json(&contents))
        //     });

        let topic_to_offset = self.topic_to_offset.clone();
        let producer_sends_message = warp::post()
            .and(warp::path("producer/messages"))
            .and(warp::body::json())
            .map(|message: listeners::ProducerSendsMessage| {
                let topic_to_offset = topic_to_offset.clone();
                let kv = format!("{}: {}", message.key, message.value);
                let filename = format!("{}-{}.log", message.topic, message.partition);
                // async {
                utils::write_to_log_file(&filename, &kv);
                // };
                let mut topic_to_offset = topic_to_offset.lock().unwrap();
                let offset = topic_to_offset.entry(message.topic.clone()).or_insert(0);
                *offset += kv.len();
                println!("Broker received message: {:?}", message);
                // };
                // Ok::<_, warp::Rejection>(warp::reply::json(&"Message received"))
                // warp::reply::json(&"Message received")
                warp::reply::json(&offset)
            });

        warp::serve(producer_sends_message)
            .run(([127, 0, 0, 1], self.addr))
            .await;
    }
}

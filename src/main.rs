use broker::Broker;
use consumer::Consumer;
use producer::Producer;
use tokio::time::sleep;

mod broker;
mod consumer;
mod producer;

#[tokio::main]
async fn main() {
    let broker_id = 8000;
    let producer_id = 8001;
    let consumer_id = 8002;

    let broker = Broker::new(broker_id, None).await;

    let mut producer = Producer::new(producer_id, broker.get_id()).await;
    producer.add_topic("example topic".to_string()).await;

    let mut consumer: Consumer = Consumer::new(consumer_id, broker.get_id()).await;
    consumer.subscribe("example topic".to_string()).await;

    // stall
    eprint!("stalling");
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}


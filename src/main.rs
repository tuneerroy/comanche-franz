use comanche_franz::consumer;

enum Service {
    Broker,
    Producer,
    Consumer,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Invalid number of arguments");
    }
    let service = match args[1].as_str() {
        "broker" => Service::Broker,
        "producer" => Service::Producer,
        "consumer" => Service::Consumer,
        _ => panic!("Invalid service"),
    };

    // start service
    match service {
        Service::Broker => {
            let addr = args[2].parse::<u16>().unwrap();
            let mut broker = comanche_franz::broker::Broker::new(addr);
            broker.listen().await;
        }
        Service::Producer => {
            let addr = 8000;
            let broker_leader_addr = args[2].parse::<u16>().unwrap();
            let mut producer = comanche_franz::producer::Producer::new(addr, broker_leader_addr).await;
            producer.add_topic("test".to_string()).await.unwrap();
            loop {
                producer.send_message("test".to_string(), "hello".to_string()).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        Service::Consumer => {
            let addr = args[2].parse::<u16>().unwrap();
            let broker_leader_addr = args[3].parse::<u16>().unwrap();
            let mut consumer = consumer::Consumer::new(addr, broker_leader_addr).await;
            consumer.join_consumer_group("group".to_string()).await.unwrap();
            consumer.subscribe("test".to_string()).await.unwrap();
            loop {
                let messages = consumer.poll().await.unwrap();
                println!("Consumer received messages: {:?}", messages);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    }
}

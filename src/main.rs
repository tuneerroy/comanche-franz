use std::io::Write;

use comanche_franz::{consumer, ServerId};

// TODO: make this more modular, and much cleaner

enum Service {
    Cluster,
    Producer,
    Consumer,
}

fn read<T: std::str::FromStr>(message: &str) -> T {
    print!("{}", message);
    std::io::stdout().flush().unwrap();
    
    let mut input = String::new();
    loop {
        std::io::stdin().read_line(&mut input).unwrap();
        match input.trim().parse::<T>() {
            Ok(n) => {
                return n;
            }
            Err(_) => {
                eprint!("Invalid input. {}", message);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Invalid number of arguments.");
    }
    let service = match args[1].as_str() {
        "cluster" => Service::Cluster,
        "producer" => Service::Producer,
        "consumer" => Service::Consumer,
        _ => panic!("Invalid service"),
    };
    match service {
        Service::Cluster => {
            let addr: ServerId = read("Enter server addr: ");
            let broker_count: usize = read("Enter number of brokers: ");
            let partition_count: usize = read("Enter number of partitions: ");

            let mut broker_lead = comanche_franz::broker_lead::BrokerLead::new(addr, broker_count, partition_count);
            eprintln!("Starting broker system...");
            broker_lead.listen().await;
        }
        Service::Producer => {
            let addr: ServerId = read("Enter server addr: ");
            let broker_leader_addr: ServerId = read("Enter broker leader addr: ");

            let mut producer = comanche_franz::producer::Producer::new(addr, broker_leader_addr).await;
            loop {
                let action: usize = read("Enter action (0: add topic, 1: remove topic, 2: send message): ");
                match action {
                    0 => {
                        let topic: String = read("Enter topic: ");
                        producer.add_topic(topic).await.unwrap();
                    }
                    1 => {
                        let topic: String = read("Enter topic: ");
                        producer.remove_topic(topic).await.unwrap();
                    }
                    2 => {
                        let topic: String = read("Enter topic: ");
                        let value: String = read("Enter value: ");
                        producer.send_message(topic, value).await.unwrap();
                    }
                    _ => {
                        eprintln!("Invalid action");
                    }
                }
            }
        }
        Service::Consumer => {
            let addr: ServerId = read("Enter server addr: ");
            let broker_leader_addr: ServerId = read("Enter broker leader addr: ");

            let mut consumer = consumer::Consumer::new(addr, broker_leader_addr);
            loop {
                let action: usize = read("Enter action (0: subscribe, 1: unsubscribe, 2: join consumer group, 3: leave consumer group): ");
                match action {
                    0 => {
                        let topic: String = read("Enter topic: ");
                        consumer.subscribe(topic).await.unwrap();
                    }
                    1 => {
                        let topic: String = read("Enter topic: ");
                        consumer.unsubscribe(topic).await.unwrap();
                    }
                    2 => {
                        let consumer_group_id: String = read("Enter consumer group id: ");
                        consumer.join_consumer_group(consumer_group_id).await.unwrap();
                    }
                    3 => {
                        consumer.leave_consumer_group().await.unwrap();
                    }
                    _ => {
                        eprintln!("Invalid action");
                    }
                }
            }
        }
    }
}

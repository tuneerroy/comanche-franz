use std::io::Write;

use comanche_franz::{
    broker::Broker, broker_lead::BrokerLead, consumer, producer::Producer, ServerId,
};

// TODO: make this more modular, and much cleaner

enum Service {
    Broker,
    BrokerLead,
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
                eprint!("Invalid input.\n{}", message);
                input.clear();
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
        "broker" => Service::Broker,
        "brokerlead" => Service::BrokerLead,
        "producer" => Service::Producer,
        "consumer" => Service::Consumer,
        _ => panic!("Invalid service"),
    };
    match service {
        Service::Broker => {
            // TODO: REMOVE THESE TEMPORARY VALUES AFTERWARDS
            let addr: ServerId = 8001;
            // let addr: ServerId = read("Enter server addr: ");

            Broker::new(addr).listen().await;
        }
        Service::BrokerLead => {
            // TODO: REMOVE THESE TEMPORARY VALUES AFTERWARDS
            let addr: ServerId = 8000;
            let partition_count: usize = 3;
            // let addr: ServerId = read("Enter server addr: ");
            // let partition_count: usize = read("Enter number of partitions: ");

            let broker_ids = vec![8001];
            // let mut broker_count: usize = read("Enter number of brokers: ");
            // while broker_count < 1 {
            //     eprintln!("Invalid number of brokers.");
            //     broker_count = read("Enter number of brokers: ");
            // }
            // let broker_ids = (0..broker_count)
            //     .map(|_| read("Enter broker addr: "))
            //     .collect();
            BrokerLead::new(addr, broker_ids, partition_count)
                .listen()
                .await;
        }
        Service::Producer => {
            // TODO: REMOVE THESE TEMPORARY VALUES AFTERWARDS
            let broker_leader_addr: ServerId = 8000;
            // let broker_leader_addr: ServerId = read("Enter broker leader addr: ");

            let mut producer = Producer::new(broker_leader_addr).await;
            loop {
                let action: usize =
                    read("Enter action (0: add topic, 1: remove topic, 2: send message): ");
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
            // TODO: REMOVE THESE TEMPORARY VALUES AFTERWARDS
            let addr: ServerId = 8080;
            let broker_leader_addr: ServerId = 8000;
            // let addr: ServerId = read("Enter server addr: ");
            // let broker_leader_addr: ServerId = read("Enter broker leader addr: ");

            let mut consumer = consumer::Consumer::new(addr, broker_leader_addr);
            loop {
                let action: usize = read("Enter action (0: subscribe, 1: unsubscribe, 2: join consumer group, 3: leave consumer group, 4: poll): ");
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
                        consumer
                            .join_consumer_group(consumer_group_id)
                            .await
                            .unwrap();
                    }
                    3 => {
                        consumer.leave_consumer_group().await.unwrap();
                    }
                    4 => {
                        let res = consumer.poll().await.unwrap();
                        eprintln!("Received message: {:?}", res);
                    }
                    _ => {
                        eprintln!("Invalid action");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn run_kafka(lead_addr: ServerId, broker_addr_start: ServerId) {
        let partition_count: usize = 3;
        let broker_count: usize = 3;
        let broker_ids = (0..broker_count)
            .map(|i| broker_addr_start + i as u16)
            .collect::<Vec<_>>();
        let broker_ids_clone = broker_ids.clone();
        tokio::spawn(async move {
            BrokerLead::new(lead_addr, broker_ids_clone, partition_count)
                .listen()
                .await;
        });
        for broker_id in broker_ids {
            tokio::spawn(async move {
                Broker::new(broker_id).listen().await;
            });
        }
    }

    #[tokio::test]
    async fn test_producer() {
        // WOULD BE SPUN UP IN BACKGROUND FOR RUNNING KAFKA, NOT BY CLIENT
        let lead_addr: ServerId = 8000;
        run_kafka(lead_addr, 8080);

        // How a client would make a producer
        let mut producer = Producer::new(lead_addr).await;
        producer.add_topic("Best foods".to_string()).await.unwrap();
        producer.send_message("Best foods".to_string(), "pizza is a good food".to_string()).await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_topics() {
        // WOULD BE SPUN UP IN BACKGROUND FOR RUNNING KAFKA, NOT BY CLIENT
        let lead_addr: ServerId = 8000;
        run_kafka(lead_addr, 8080);

        // How a client would make a producer
        let mut producer = Producer::new(lead_addr).await;
        producer.add_topic("Best foods".to_string()).await.unwrap();
        producer.add_topic("Best drinks".to_string()).await.unwrap();
        producer.send_message("Best foods".to_string(), "pizza is a good food".to_string()).await.unwrap();
        producer.send_message("Best drinks".to_string(), "water is the only good drink".to_string()).await.unwrap();
        producer.send_message("Best foods".to_string(), "chicken is a good food".to_string()).await.unwrap();
        producer.send_message("Best drinks".to_string(), "milk is a good drink".to_string()).await.unwrap();
    }

    #[tokio::test]
    async fn test_producer_consumer() {
        // WOULD BE SPUN UP IN BACKGROUND FOR RUNNING KAFKA, NOT BY CLIENT
        let lead_addr: ServerId = 8000;
        run_kafka(lead_addr, 8080);

        // How a client would make a producer
        let mut producer = Producer::new(lead_addr).await;
        producer.add_topic("Best foods".to_string()).await.unwrap();


        let mut consumer = consumer::Consumer::new(8001, lead_addr);
        let res = consumer.poll().await.unwrap();
        let message = format!("{:?}", res);
        assert_eq!(message, "None"); // fix this

        producer.send_message("Best foods".to_string(), "pizza is a good food".to_string()).await.unwrap();

        let res = consumer.poll().await.unwrap();
        let message = format!("{:?}", res);
        assert_eq!(message, "None"); // fix this

        consumer.subscribe("Best foods".to_string()).await.unwrap();
        let res = consumer.poll().await.unwrap();
        let message = format!("{:?}", res);
        assert_eq!(message, "Some(\"pizza is a good food\")"); // fix this
    }
}

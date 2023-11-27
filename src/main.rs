use broker::Broker;
use comanche_franz::Service;
use consumer::Consumer;
use producer::Producer;

mod producer;
mod consumer;
mod broker;

// TODO: figure out what stateful data needs to be contained for a given service
enum ServiceType {
    Producer,
    Consumer,
    Broker
}

struct ServerConfig {
    address: String,
    port: u16,
    _replicas: u8, // TODO: use for scaling up horizontally
}

fn parse_arguments() -> Result<(ServiceType, ServerConfig), &'static str> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 5 {
        return Err("Need arguments for server type, address, port, and num replicas")
    }

    let server_type = &args[1];
    let address = &args[2].to_string();
    let port = &args[3].parse::<u16>().map_err(|_| "Failed to parse port")?;
    let replicas = &args[4].parse::<u8>().map_err(|_| "Failed to parse num replicas")?;

    let server_config = ServerConfig {
        address: address.to_string(),
        port: *port,
        _replicas: *replicas,
    };

    match server_type.as_str() {
        "producer" => Ok((ServiceType::Producer, server_config)),
        "consumer" => Ok((ServiceType::Consumer, server_config)),
        "broker" => Ok((ServiceType::Broker, server_config)),
        _ => Err("Invalid server type"),
    }
}

fn get_service(service_type: ServiceType, listener: std::net::TcpListener) -> Box<dyn Service> {
    match service_type {
        ServiceType::Producer => {
            Box::new(Producer::new(listener))
        },
        ServiceType::Consumer => {
            Box::new(Consumer::new(listener))
        },
        ServiceType::Broker => {
            Box::new(Broker::new(listener))
        },
    }
}

fn main() -> Result<(), &'static str> {
    let (service, server) = parse_arguments()?;

    // start server
    let tcp_connection = std::net::TcpListener::bind(format!("{}:{}", server.address, server.port));
    if tcp_connection.is_err() {
        return Err("Failed to bind to address");
    }
    let listener = tcp_connection.unwrap();
    let _service = get_service(service, listener);

    loop {
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).expect("Failed to read from stdin");
        print!("You typed: {}", buffer);
    }
}

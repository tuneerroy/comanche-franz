mod broker;
mod producer;
mod consumer;

// fn setup() {
//     thread::spawn(|| {
//         server::run_server(ADDRESS);
//     });
// }

// TODO: figure out what stateful data needs to be contained for a given service
enum Service {
    Producer,
    Consumer,
    Broker
}

struct ServerConfig {
    address: String,
    port: u16,
    _replicas: u8, // TODO: use for scaling up horizontally
}

fn parse_arguments() -> Result<(Service, ServerConfig), &'static str> {
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
        "producer" => Ok((Service::Producer, server_config)),
        "consumer" => Ok((Service::Consumer, server_config)),
        "broker" => Ok((Service::Broker, server_config)),
        _ => Err("Invalid server type"),
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

    match service {
        Service::Producer => {
            println!("Starting producer service on {}:{}", server.address, server.port);
            producer::run_service(listener);
        },
        Service::Consumer => {
            println!("Starting consumer service on {}:{}", server.address, server.port);
            consumer::run_service(listener);
        },
        Service::Broker => {
            println!("Starting broker service on {}:{}", server.address, server.port);
            broker::run_service(listener);
        },
    }

    loop {
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).expect("Failed to read from stdin");
        println!("You typed: {}", buffer);
    }
}

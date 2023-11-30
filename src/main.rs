use broker::Broker;
use comanche_franz::Service;
use consumer::Consumer;
use producer::Producer;

mod producer;
mod consumer;
mod broker;

async fn parse_arguments() -> Result<Box<dyn Service>, &'static str> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        return Err("Need arguments for server type")
    }

    let server_type = &args[1];
    let terminal_args = args[2..].to_vec();
    match server_type.as_str() {
        "producer" => Ok(Box::new(Producer::from_terminal(terminal_args).await?)),
        "consumer" => Ok(Box::new(Consumer::from_terminal(terminal_args).await?)),
        "broker" => Ok(Box::new(Broker::from_terminal(terminal_args).await?)),
        _ => return Err("Invalid server type"),
    }
}

#[tokio::main]
async fn main() -> Result<(), &'static str> {
    let _service = parse_arguments().await?;
    loop {
        let mut buffer = String::new();
        std::io::stdin().read_line(&mut buffer).expect("Failed to read from stdin");
        print!("You typed: {}", buffer);
    }
}

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

// struct Topic(String);

// struct TopicToPartitions {
//     topic: String,
//     partitions: Vec<ServerId>,
// }

// pub async fn write_to_log_file(filename: String, message: String) -> () {
//     let mut file = OpenOptions::new()
//         .append(true)
//         .create(true)
//         .open(filename)
//         .await
//         .expect("Failed to open file");

//     file.write_all(message.as_bytes())
//         .await
//         .expect("Failed to write to file");
// }

// pub async fn read_from_file(filename: String, offset: u128) -> String {
//     let mut file = OpenOptions::new()
//         .read(true)
//         .open(filename)
//         .await
//         .expect("Failed to open file");

//     let mut contents = String::new();
//     file.read_to_string(&mut contents)
//         .await
//         .expect("Failed to read from file");

//     contents[offset..].to_string();
// }

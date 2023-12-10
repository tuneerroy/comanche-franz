// pub async fn add_topic(
//     topic: String,
//     broker_leader_addr: String,
// ) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
//     let mut res = reqwest::Client::new()
//         .post(format!("http://{}/add_topic", broker_leader_addr))
//         .json(&topic)
//         .send()
//         .await?;

//     let partitions: Vec<u16> = res.json().await?;
//     Ok(partitions)
// }

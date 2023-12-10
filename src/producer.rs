use std::collections::HashMap;

pub struct Producer {
    addr: u16,
    broker_leader_addr: u16,
    topic_to_partitions: HashMap<String, (usize, Vec<u16>)>, // (partition index for roundable, brokers)
}

impl Producer {
    pub async fn new(id: u16, broker_leader: u16) -> Producer {
        Producer {
            addr: id,
            broker_leader_addr: broker_leader,
            topic_to_partitions: HashMap::new(),
        }
    }

    pub async fn add_topic(&mut self, topic: String) -> Result<(), reqwest::Error> {
        if self.topic_to_partitions.contains_key(&topic) {
            return Ok(());
        }

        let res = reqwest::Client::new()
            .post(format!(
                "http://localhost:{}/{}/topics",
                self.broker_leader_addr, self.addr
            ))
            .json(&topic)
            .send()
            .await?;

        let partitions: Vec<u16> = res.json().await?;
        self.topic_to_partitions.insert(topic, (0, partitions));
        Ok(())
    }

    pub async fn remove_topic(&mut self, topic: String) -> Result<(), reqwest::Error> {
        if !self.topic_to_partitions.contains_key(&topic) {
            return Ok(());
        }

        reqwest::Client::new()
            .delete(format!(
                "http://localhost:{}/{}/topics/{}",
                self.broker_leader_addr, self.addr, topic
            ))
            .send()
            .await?;

        self.topic_to_partitions.remove(&topic);
        Ok(())
    }

    pub async fn send_message(
        &mut self,
        topic: String,
        value: String,
    ) -> Result<(), reqwest::Error> {
        if !self.topic_to_partitions.contains_key(&topic) {
            return Ok(());
        }

        let (partition_index, brokers) = self.topic_to_partitions.get_mut(&topic).unwrap();
        let broker = brokers[*partition_index];
        reqwest::Client::new()
            .post(format!("http://localhost:{}/{}/messages", broker, topic))
            .json(&value)
            .send()
            .await?;

        // increment partition index
        *partition_index += 1;

        Ok(())
    }
}

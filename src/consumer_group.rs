use std::collections::HashSet;

pub struct ConsumerGroupId(u32);



pub struct ConsumerGroup {
    pub groupId : ConsumerGroupId,
    broker_lead : &BrokerLeader,
    consumers : Vec<Consumer>,
    topics : HashSet<String>
}

// impl ConsumerGroup {
//     pub fn add_consumer(&mut self, new_consumer : Consumer) {
//         // self.consumers.push(new_consumer);
//         for topic in new_consumer.get_topics() {
//             if !self.topics.contains(topic) {
                
//                 // add all partitions from topic to consumer
//             } else {
//                 self.topics.insert(topic);
//                 let partitions_per_consumer = self.broker_lead.num_partitions() / (consumers.len() + 1);
//                 for consumer in self.consumers {
//                     for stream in consumer.split_off_partitions(partitions_per_consumer) {
//                         new_consumer.push_stream(stream);
//                     }
//                 }
//             }
//         }
//     }

//     pub fn remove_consumer(&consumer : Consumer) {

//     }
// }
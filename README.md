# Comanche Franz

## An attempt at a Kafka client + cluster manager in Rust

#### [NOTE] Much work was doing using VSCode's Live Share feature, so the commit history is not an accurate representation of the work done.

Welcome to our final project! Here is a list of the key feature in this project, and the structure of our Kafka service.

First of all, the service is split into the following parts:
- Broker Leader
- Broker
- Producer
- Consumer

The broker leader is responsible for coordinating all of the other brokers, and it also assigns the partitions and sends this info out.

Here is a diagram demonstrating what parts talk to what:
..AD HERE..

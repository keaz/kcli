use std::{
    collections::HashMap,
    io::{Cursor, Read},
};

use byteorder::{BigEndian, ReadBytesExt};
use prettytable::{row, Table};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    error::KafkaResult,
    groups::GroupList,
    metadata::Metadata,
    ClientConfig, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};

fn get_consumer(bootstrap_servers: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("Consumer creation failed");

    consumer
}

pub fn get_topics(bootstrap_servers: &str) {
    let consumer = get_consumer(bootstrap_servers);
    let metadata: KafkaResult<Metadata> =
        consumer.fetch_metadata(None, std::time::Duration::from_secs(10));

    match metadata {
        Ok(metadata) => {
            let mut table = Table::new();
            table.add_row(row!["Topic", "Partitions"]);
            metadata.topics().iter().for_each(|t| {
                table.add_row(row![t.name(), t.partitions().len(),]);
            });
            table.printstd();
        }
        Err(e) => {
            println!("Error while getting topics: {:?}", e);
        }
    }
}

pub fn get_topic_detail(bootstrap_servers: &str, topic: &str) {
    let consumer = get_consumer(bootstrap_servers);
    let topic_detail =
        consumer.fetch_metadata(Option::Some(topic), std::time::Duration::from_secs(10));

    match topic_detail {
        Ok(topics) => {
            topics.topics().iter().for_each(|t| {
                let mut overall_detail = Table::new();
                overall_detail.add_row(row!["Partitions", "Partition IDs", "Total Messages"]);
                let partition_count = t.partitions().len();
                let mut total_messages = 0;
                let mut partition_ids = String::new();

                let mut partition_detail = Table::new();
                partition_detail.add_row(row!["Partition ID", "Leader", "Offset"]);
                t.partitions().iter().for_each(|p| {
                    partition_ids.push_str(&p.id().to_string());
                    partition_ids.push_str(", ");

                    let mut tpl = TopicPartitionList::new();
                    tpl.add_partition_offset(topic, p.id(), Offset::End)
                        .unwrap();
                    let offsets = consumer
                        .offsets_for_times(tpl, std::time::Duration::from_secs(10))
                        .expect("Failed to get offsets");

                    let mut partion_offset = 0;
                    if let Some(offset) = offsets.elements_for_topic(topic).first() {
                        if let Offset::Offset(offset) = offset.offset() {
                            total_messages += offset;
                            partion_offset = offset;
                        }
                    }
                    partition_detail.add_row(row![p.id(), p.leader(), partion_offset]);
                });

                overall_detail.add_row(row![partition_count, partition_ids, total_messages]);
                overall_detail.printstd();
                partition_detail.printstd();
                list_consumers_for_topic(&consumer, topic);
            });
        }
        Err(e) => {
            println!("Error while getting topic detail: {:?}", e);
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Assignment {
    topic: String,
    partitions: Vec<i32>,
}

fn deserialize_assignment(data: &[u8]) -> HashMap<String, Vec<i32>> {
    let mut assignments = HashMap::new();
    let mut cursor = Cursor::new(data);

    // Read the version
    let _version = cursor.read_i16::<BigEndian>().unwrap();

    // Read the number of topics
    let topic_count = cursor.read_i32::<BigEndian>().unwrap();
    for _ in 0..topic_count {
        // Read the topic name
        let topic_len = cursor.read_i16::<BigEndian>().unwrap() as usize;
        let mut topic_bytes = vec![0; topic_len];
        cursor.read_exact(&mut topic_bytes).unwrap();
        let topic = String::from_utf8(topic_bytes).unwrap();

        // Read the number of partitions
        let partition_count = cursor.read_i32::<BigEndian>().unwrap();
        let mut partitions = Vec::new();
        for _ in 0..partition_count {
            let partition = cursor.read_i32::<BigEndian>().unwrap();
            partitions.push(partition);
        }

        assignments.insert(topic, partitions);
    }

    assignments
}

pub fn list_consumers_for_topic(consumer: &BaseConsumer, topic: &str) {
    let group_list: KafkaResult<GroupList> =
        consumer.fetch_group_list(None, std::time::Duration::from_secs(10));

    match group_list {
        Ok(groups) => {
            println!("Consumer Groups consuming from topic '{}':", topic);
            for group in groups.groups() {
                let mut is_consuming = false;
                for member in group.members() {
                    let assignment = deserialize_assignment(member.assignment().unwrap());
                    if assignment.contains_key(topic) {
                        is_consuming = true;
                        break;
                    }
                }

                if is_consuming {
                    let mut table = Table::new();
                    table.add_row(row!["Group ID", "State", "Protocol Type", "Protocol"]);

                    table.add_row(row![
                        group.name(),
                        group.state(),
                        group.protocol_type(),
                        group.protocol()
                    ]);
                    for member in group.members() {
                        let assignment = deserialize_assignment(member.assignment().unwrap());
                        if assignment.contains_key(topic) {
                            // println!("Member ID: {}", member.id());
                            // println!("Client ID: {}", member.client_id());
                            // println!("Client Host: {}", member.client_host());
                            // println!("Assignment: {:?}", assignment);
                            // println!("Metadata: {:?}", member.metadata());
                        }
                    }
                    table.printstd();
                }
            }
        }
        Err(e) => {
            println!("Error while listing consumer groups: {:?}", e);
        }
    }
}

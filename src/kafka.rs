use std::{
    collections::HashMap,
    f32::consts::E,
    fmt::Debug,
    io::{Cursor, Read},
    time::Duration,
};

use byteorder::{BigEndian, ReadBytesExt};
use colored_json::to_colored_json_auto;
use prettytable::{row, Table};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    error::KafkaResult,
    groups::GroupList,
    metadata::{Metadata, MetadataPartition},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use toml::Value;

const GROUP_ID: &str = "kfcli";

fn get_consumer(bootstrap_servers: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", GROUP_ID)
        .set("auto.offset.reset", "latest")
        .create()
        .expect("Consumer creation failed");

    consumer
}

fn get_given_consumer(bootstrap_servers: &str, group_id: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "latest")
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

    get_topic_detail_inner(&consumer, topic)
        .map(
            |(overall_header, overall_detail, partition_detail_header, partition_detail)| {
                let mut overall_table = Table::new();
                overall_table.add_row(row![
                    overall_header[0],
                    overall_header[1],
                    overall_header[2]
                ]);
                overall_table.add_row(row![
                    overall_detail[0],
                    overall_detail[1],
                    overall_detail[2]
                ]);
                overall_table.printstd();

                let mut partition_table = Table::new();
                partition_table.add_row(row![
                    partition_detail_header[0],
                    partition_detail_header[1],
                    partition_detail_header[2]
                ]);
                for row in partition_detail {
                    partition_table.add_row(row![row[0], row[1], row[2]]);
                }
                partition_table.printstd();
            },
        )
        .unwrap_or_else(|e| println!("Error while getting topic detail: {:?}", e));

    list_consumers_for_topic(&consumer, topic);
}

fn get_topic_detail_inner<'a>(
    consumer: &'a BaseConsumer,
    topic: &'a str,
) -> Result<([&'a str; 3], [String; 3], [&'a str; 3], Vec<[String; 3]>), Error> {
    let topic_detail =
        consumer.fetch_metadata(Option::Some(topic), std::time::Duration::from_secs(10));

    let overall_header = ["Partitions", "Partition IDs", "Total Messages"];
    let mut overall_detail = [String::from("0"), String::from(""), String::from("0")];
    let partition_detail_header = ["Partition ID", "Leader", "Offset"];
    let mut partition_detail = vec![];

    match topic_detail {
        Ok(topics) => {
            topics.topics().iter().for_each(|t| {
                let partition_count = t.partitions().len();
                let mut total_messages = 0;
                let mut partition_ids = String::new();

                t.partitions().iter().for_each(|p| {
                    partition_detail_inner(
                        &mut partition_ids,
                        p,
                        topic,
                        consumer,
                        &mut total_messages,
                        &mut partition_detail,
                    );
                });

                overall_detail = [
                    partition_count.to_string(),
                    partition_ids,
                    total_messages.to_string(),
                ];
            });
            Ok((
                overall_header,
                overall_detail,
                partition_detail_header,
                partition_detail,
            ))
        }
        Err(e) => Err(Error::Topic(e)),
    }
}

fn partition_detail_inner(
    partition_ids: &mut String,
    p: &MetadataPartition,
    topic: &str,
    consumer: &BaseConsumer,
    total_messages: &mut i64,
    partition_detail: &mut Vec<[String; 3]>,
) {
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
            *total_messages += offset;
            partion_offset = offset;
        }
    }
    partition_detail.push([
        p.id().to_string(),
        p.leader().to_string(),
        partion_offset.to_string(),
    ]);
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
            for group in groups.groups() {
                let mut is_consuming = false;
                if group.state() == "Stable" {
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
                        table.printstd();

                        for member in group.members() {
                            let assignment = deserialize_assignment(member.assignment().unwrap());
                            if assignment.contains_key(topic) {}
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error while listing consumer groups: {:?}", e);
        }
    }
}

pub fn tail_topic(bootstrap_servers: &str, topic: &str, filter: Option<String>) {
    let consumer = get_consumer(bootstrap_servers);

    consumer
        .subscribe(&[topic])
        .expect("Failed to subscribe to topic");

    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                let payload = message
                    .payload_view::<str>()
                    .unwrap_or(Ok(""))
                    .unwrap_or("");
                let _ = message.key_view::<str>().unwrap_or(Ok("")).unwrap_or("");

                if let Ok(json) = serde_json::from_str::<Value>(payload) {
                    if let Some(filter) = &filter {
                        if apply_filter(&json, filter) {
                            let colored_json = colorize_json(&json);
                            println!("{}", colored_json);
                        }
                    } else {
                        let colored_json = colorize_json(&json);
                        println!("{}", colored_json);
                    }
                }
            }
            Some(Err(e)) => {
                println!("Error while consuming message: {:?}", e);
            }
            None => {
                // No message received, continue polling
            }
        }
    }
}

fn apply_filter(json: &Value, filter: &str) -> bool {
    let parts: Vec<&str> = filter.split('=').collect();
    let path = parts[0];
    let path_parts: Vec<&str> = path.split('.').collect();
    let mut current = json;

    for part in path_parts {
        match current.get(part) {
            Some(value) => current = value,
            None => return false,
        }
    }

    if parts.len() == 2 {
        let expected_value = parts[1];
        let current_value = current.to_string().replace("\"", "");
        return current_value == expected_value;
    }

    true
}

fn colorize_json(json: &Value) -> String {
    to_colored_json_auto(json).unwrap_or_else(|_| "Invalid JSON".to_string())
}

pub fn get_broker_detail(bootstrap_servers: &str) {
    let consumer = get_consumer(bootstrap_servers);
    let metadata: KafkaResult<Metadata> =
        consumer.fetch_metadata(None, std::time::Duration::from_secs(10));

    match metadata {
        Ok(metadata) => {
            let mut table = Table::new();
            table.add_row(row!["Broker ID", "Host", "Port"]);
            metadata.brokers().iter().for_each(|b| {
                table.add_row(row![b.id(), b.host(), b.port()]);
            });
            table.printstd();
        }
        Err(e) => {
            println!("Error while getting brokers: {:?}", e);
        }
    }

    get_broker_detail_inner(bootstrap_servers)
        .map(|(headers, rows)| print_broker_table(&headers, &rows))
        .unwrap_or_else(|e| println!("Error while getting brokers: {:?}", e));
}

fn get_broker_detail_inner(
    bootstrap_servers: &str,
) -> Result<([&str; 3], Vec<[String; 3]>), Error> {
    let consumer = get_consumer(bootstrap_servers);
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10));

    match metadata {
        Ok(metadata) => {
            let headers = ["Broker ID", "Host", "Port"];
            let rows: Vec<[String; 3]> = metadata
                .brokers()
                .iter()
                .map(|b| {
                    [
                        b.id().to_string(),
                        b.host().to_string(),
                        b.port().to_string(),
                    ]
                })
                .collect();
            Ok((headers, rows))
        }
        Err(e) => Err(Error::Broker(e)),
    }
}

fn print_broker_table(headers: &[&str; 3], rows: &[[String; 3]]) {
    let mut table = Table::new();
    table.add_row(row![headers[0], headers[1], headers[2]]);
    for row in rows {
        table.add_row(row![row[0], row[1], row[2]]);
    }
    table.printstd();
}

pub fn get_consumer_groups(bootstrap_servers: &str) {
    get_consumer_groups_inner(bootstrap_servers)
        .map(|(headers, rows)| print_consumer_groups_table(&headers, &rows))
        .unwrap_or_else(|e| println!("Error while getting consumer groups: {:?}", e));
}

fn get_consumer_groups_inner(
    bootstrap_servers: &str,
) -> Result<([&str; 4], Vec<[String; 4]>), Error> {
    let consumer = get_consumer(bootstrap_servers);
    let group_list = consumer.fetch_group_list(None, Duration::from_secs(10));

    match group_list {
        Ok(groups) => {
            let headers = ["Group ID", "State", "Protocol Type", "Protocol"];

            let rows: Vec<[String; 4]> = groups
                .groups()
                .iter()
                .map(|g| {
                    [
                        g.name().to_string(),
                        g.state().to_string(),
                        g.protocol_type().to_string(),
                        g.protocol().to_string(),
                    ]
                })
                .collect();
            Ok((headers, rows))
        }
        Err(e) => Err(Error::Group(e)),
    }
}

fn print_consumer_groups_table(headers: &[&str; 4], rows: &[[String; 4]]) {
    let mut table = Table::new();
    table.add_row(row![headers[0], headers[1], headers[2], headers[3]]);
    for row in rows {
        table.add_row(row![row[0], row[1], row[2], row[3]]);
    }
    table.printstd();
}

pub fn get_consumers_group_details(bootstrap_servers: &str, group: String, lag: bool) {
    get_consumers_group_details_inner(bootstrap_servers, &group)
        .map(
            |(group_header, group_detail, member_header, member_detail)| {
                let mut group_table = Table::new();
                group_table.add_row(row![
                    group_header[0],
                    group_header[1],
                    group_header[2],
                    group_header[3]
                ]);
                group_table.add_row(row![
                    group_detail[0],
                    group_detail[1],
                    group_detail[2],
                    group_detail[3]
                ]);
                group_table.printstd();

                let mut member_table = Table::new();
                member_table.add_row(row![
                    member_header[0],
                    member_header[1],
                    member_header[2],
                    member_header[3],
                    member_header[4]
                ]);
                member_table.add_row(row![
                    member_detail[0],
                    member_detail[1],
                    member_detail[2],
                    member_detail[3],
                    member_detail[4]
                ]);
                member_table.printstd();
            },
        )
        .unwrap_or_else(|e| println!("Error while getting consumer group details: {:?}", e));

    if lag {
        calculate_consumer_lag(bootstrap_servers, &group)
            .unwrap_or_else(|e| println!("Error while calculating consumer lag: {:?}", e));
    }
}

fn get_consumers_group_details_inner<'a>(
    bootstrap_servers: &str,
    group: &'a str,
) -> Result<([&'a str; 4], [String; 4], [&'a str; 5], [String; 5]), Error> {
    let consumer = get_consumer(bootstrap_servers);
    let group_list: KafkaResult<GroupList> =
        consumer.fetch_group_list(Some(&group), std::time::Duration::from_secs(10));

    let group_header = ["Group ID", "State", "Protocol Type", "Protocol"];
    let mut group_detail = [
        String::from(""),
        String::from(""),
        String::from(""),
        String::from(""),
    ];

    let member_header = ["Member ID", "Client ID", "Host", "Topoc", "Partitions"];
    let mut member_detail = [
        String::from(""),
        String::from(""),
        String::from(""),
        String::from(""),
        String::from(""),
    ];
    match group_list {
        Ok(groups) => {
            for group in groups.groups() {
                group_detail = [
                    group.name().to_string(),
                    group.state().to_string(),
                    group.protocol_type().to_string(),
                    group.protocol().to_string(),
                ];

                if group.state() == "Stable" {
                    for member in group.members() {
                        let assignment = deserialize_assignment(member.assignment().unwrap());

                        for (topic, partitions) in assignment {
                            let partitions = partitions
                                .iter()
                                .map(|p| p.to_string())
                                .collect::<Vec<String>>();
                            member_detail = [
                                member.id().to_string(),
                                member.client_id().to_string(),
                                member.client_host().to_string(),
                                topic,
                                partitions.join(", "),
                            ];

                            // get_topic_detail_inner(&consumer, &topic);
                        }
                    }
                }
            }
            Ok((group_header, group_detail, member_header, member_detail))
        }
        Err(e) => {
            return Err(Error::Group(e));
        }
    }
}

fn calculate_consumer_lag(bootstrap_servers: &str, group_id: &str) -> Result<(), Error> {
    let consumer = get_given_consumer(bootstrap_servers, group_id);

    let subscription = consumer.subscription();
    if subscription.is_err() {
        return Err(Error::Group(subscription.err().unwrap()));
    }

    // #TODO: This should be implemented
    // let subscription = subscription.unwrap();
    // let topics: Vec<String> = subscription
    //     .elements()
    //     .iter()
    //     .map(|tp| tp.topic().to_string())
    //     .collect();

    // Get metadata for topic partitions
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .map_err(Error::Topic)?;

    for topic in metadata.topics() {
        let topic_metadata = topic;
        println!("Topic: {}", topic_metadata.name());

        // Create partition list for committed offsets
        let mut tpl = TopicPartitionList::new();
        for partition in topic_metadata.partitions() {
            tpl.add_partition(topic_metadata.name(), partition.id());
        }

        let mut table = Table::new();
        table.add_row(row!["Partition", "Current Offset", "Latest Offset", "Lag"]);

        let mut total_messages = 0;
        let mut partition_detail: Vec<[String; 3]> = vec![];

        for partition in topic_metadata.partitions() {
            let partition_id = partition.id();

            partition_detail_inner(
                &mut String::new(),
                partition,
                topic_metadata.name(),
                &consumer,
                &mut total_messages,
                &mut partition_detail,
            );

            // Get latest offset
            let (_, high_watermark) = consumer
                .fetch_watermarks(topic_metadata.name(), partition_id, Duration::from_secs(5))
                .map_err(Error::Topic)?;

            let mut topic_partition_list = TopicPartitionList::new();
            topic_partition_list.add_partition(topic_metadata.name(), partition_id);
            let committed_offsets = consumer
                .committed_offsets(topic_partition_list, std::time::Duration::from_secs(5))
                .expect("Failed to fetch committed offsets");

            // Get committed offset
            let committed_offset = committed_offsets
                .find_partition(topic_metadata.name(), partition_id)
                .and_then(|p| Some(p.offset().to_raw()))
                .unwrap_or(Some(0))
                .unwrap_or(0);

            // Calculate lag
            let lag = high_watermark - committed_offset;

            table.add_row(row![partition_id, committed_offset, high_watermark, lag]);
        }
        table.printstd();
    }

    Ok(())
}

pub enum Error {
    Broker(rdkafka::error::KafkaError),
    Group(rdkafka::error::KafkaError),
    Topic(rdkafka::error::KafkaError),
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Broker(e) => write!(f, "{:?}", e),
            Error::Group(e) => write!(f, "{:?}", e),
            Error::Topic(e) => write!(f, "{:?}", e),
        }
    }
}

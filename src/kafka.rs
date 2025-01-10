use std::{
    collections::HashMap,
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
    metadata::{Metadata, MetadataPartition},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use toml::Value;

const GROUP_ID: &str = "kfcli";

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("{0}")]
    MetadataFetch(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    Generic(String),

    #[error("{0}")]
    OffsetFetch(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    Deserialize(String, #[source] std::io::Error),

    #[error("{0}")]
    GroupListFetch(String, #[source] rdkafka::error::KafkaError),
}

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

pub fn get_topics(bootstrap_servers: &str) -> Result<(), KafkaError> {
    let metadata = get_topics_inner(bootstrap_servers, None).map_err(|er| {
        if let rdkafka::error::KafkaError::MetadataFetch(_) = er {
            KafkaError::MetadataFetch("Error while fetching topic metadata".to_string(), er)
        } else {
            KafkaError::Generic("Error while fetching topics".to_string())
        }
    })?;
    let mut table = Table::new();
    table.add_row(row!["Topic", "Partitions"]);
    metadata.topics().iter().for_each(|t| {
        table.add_row(row![t.name(), t.partitions().len(),]);
    });
    table.printstd();
    Ok(())
}

fn get_topics_inner(
    bootstrap_servers: &str,
    topic: Option<&str>,
) -> Result<Metadata, rdkafka::error::KafkaError> {
    let consumer = get_consumer(bootstrap_servers);
    consumer.fetch_metadata(topic, Duration::from_secs(10))
}

pub fn get_topic_detail(bootstrap_servers: &str, topic: &str) -> Result<(), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);

    get_topic_detail_inner(&consumer, topic).map(
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
    )?;

    list_consumers_for_topic(&consumer, topic)?;

    Ok(())
}

fn get_topic_detail_inner<'a>(
    consumer: &'a BaseConsumer,
    topic: &'a str,
) -> Result<([&'a str; 3], [String; 3], [&'a str; 3], Vec<[String; 3]>), KafkaError> {
    let topic_detail = consumer
        .fetch_metadata(Option::Some(topic), std::time::Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::MetadataFetch(_) = er {
                KafkaError::MetadataFetch("Error while fetching topic metadata".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching topics".to_string())
            }
        })?;

    let overall_header = ["Partitions", "Partition IDs", "Total Messages"];
    let mut overall_detail = [String::from("0"), String::from(""), String::from("0")];
    let partition_detail_header = ["Partition ID", "Leader", "Offset"];
    let mut partitions_detail = vec![];

    topic_detail.topics().iter().for_each(|t| {
        let partition_count = t.partitions().len();

        let (partition_ids, partition_detail, total_messages) = t.partitions().iter().fold(
            (String::new(), vec![], 0),
            |(mut partition_ids, mut partition_detail, mut total_messages), p| {
                let partition_result = partition_detail_inner(p, topic, consumer);
                if let Ok((ids, detail, messages)) = partition_result {
                    partition_ids.push_str(&ids);
                    partition_ids.push_str(", ");
                    total_messages += messages;
                    partition_detail.extend(detail);
                } else {
                    partition_ids.push_str("Error");
                    partition_ids.push_str(", ");
                }
                (partition_ids, partition_detail, total_messages)
            },
        );

        partitions_detail = partition_detail;

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
        partitions_detail,
    ))
}

fn partition_detail_inner(
    p: &MetadataPartition,
    topic: &str,
    consumer: &BaseConsumer,
) -> Result<(String, Vec<[String; 3]>, i64), KafkaError> {
    let mut partition_ids = String::new();
    let mut partition_detail = vec![];
    let mut total_messages = 0;

    partition_ids.push_str(&p.id().to_string());
    partition_ids.push_str(", ");

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, p.id(), Offset::End)
        .unwrap();
    let offsets = consumer
        .offsets_for_times(tpl, std::time::Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::OffsetFetch(_) = er {
                KafkaError::OffsetFetch("Error while fetching partition offsets".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching partition offsets".to_string())
            }
        })?;

    let mut partion_offset = 0;
    if let Some(offset) = offsets.elements_for_topic(topic).first() {
        if let Offset::Offset(offset) = offset.offset() {
            total_messages = offset;
            partion_offset = offset;
        }
    }
    partition_detail.push([
        p.id().to_string(),
        p.leader().to_string(),
        partion_offset.to_string(),
    ]);

    Ok((partition_ids, partition_detail, total_messages))
}

#[derive(Debug, Deserialize, Serialize)]
struct Assignment {
    topic: String,
    partitions: Vec<i32>,
}

fn deserialize_assignment(data: &[u8]) -> Result<HashMap<String, Vec<i32>>, KafkaError> {
    let mut assignments = HashMap::new();
    let mut cursor = Cursor::new(data);

    // Read the version
    let _version = cursor.read_i16::<BigEndian>().map_err(|er| {
        KafkaError::Deserialize(format!("Error while reading assignment version:"), er)
    })?;

    // Read the number of topics
    let topic_count = cursor
        .read_i32::<BigEndian>()
        .map_err(|er| KafkaError::Deserialize(format!("Error while reading topic count:"), er))?;

    for _ in 0..topic_count {
        // Read the topic name
        let topic_len = cursor.read_i16::<BigEndian>().map_err(|er| {
            KafkaError::Deserialize(format!("Error while reading topic length:"), er)
        })? as usize;

        let mut topic_bytes = vec![0; topic_len];
        cursor.read_exact(&mut topic_bytes).map_err(|er| {
            KafkaError::Deserialize(format!("Error while reading topic name:"), er)
        })?;

        let topic = String::from_utf8(topic_bytes).map_err(|er| {
            KafkaError::Generic(format!("Error while converting topic name: {:?}", er))
        })?;

        // Read the number of partitions
        let partition_count = cursor.read_i32::<BigEndian>().map_err(|er| {
            KafkaError::Deserialize(format!("Error while reading partition count:"), er)
        })?;
        let mut partitions = Vec::new();
        for _ in 0..partition_count {
            let partition = cursor.read_i32::<BigEndian>().map_err(|er| {
                KafkaError::Deserialize(format!("Error while reading partition:"), er)
            })?;
            partitions.push(partition);
        }

        assignments.insert(topic, partitions);
    }

    Ok(assignments)
}

pub fn list_consumers_for_topic(consumer: &BaseConsumer, topic: &str) -> Result<(), KafkaError> {
    let groups = consumer
        .fetch_group_list(None, std::time::Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::GroupListFetch(_) = er {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            } else {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            }
        })?;

    for group in groups.groups() {
        let mut is_consuming = false;
        if group.state() == "Stable" {
            for member in group.members() {
                let assignment = member.assignment();
                if assignment.is_none() {
                    continue;
                }
                let assignment = deserialize_assignment(assignment.unwrap())?;
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
                    let assignment = member.assignment();
                    if assignment.is_none() {
                        continue;
                    }
                    let assignment = deserialize_assignment(assignment.unwrap())?;
                    if assignment.contains_key(topic) {}
                }
            }
        }
    }
    Ok(())
}

pub fn tail_topic(
    bootstrap_servers: &str,
    topic: &str,
    filter: Option<String>,
) -> Result<(), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);

    consumer
        .subscribe(&[topic])
        .map_err(|er| KafkaError::Generic(format!("Error while subscribing to topic: {:?}", er)))?;

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
                Err(KafkaError::Generic(format!("Error while polling: {:?}", e)))?;
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

pub fn get_broker_detail(bootstrap_servers: &str) -> Result<(), KafkaError> {
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
        .map(|(headers, rows)| print_broker_table(&headers, &rows))?;

    Ok(())
}

fn get_broker_detail_inner(
    bootstrap_servers: &str,
) -> Result<([&str; 3], Vec<[String; 3]>), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::MetadataFetch(_) = er {
                KafkaError::MetadataFetch("Error while fetching broker metadata".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching brokers".to_string())
            }
        })?;

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

fn print_broker_table(headers: &[&str; 3], rows: &[[String; 3]]) {
    let mut table = Table::new();
    table.add_row(row![headers[0], headers[1], headers[2]]);
    for row in rows {
        table.add_row(row![row[0], row[1], row[2]]);
    }
    table.printstd();
}

pub fn get_consumer_groups(bootstrap_servers: &str) -> Result<(), KafkaError> {
    get_consumer_groups_inner(bootstrap_servers)
        .map(|(headers, rows)| print_consumer_groups_table(&headers, &rows))?;
    Ok(())
}

fn get_consumer_groups_inner(
    bootstrap_servers: &str,
) -> Result<([&str; 4], Vec<[String; 4]>), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);
    let groups = consumer
        .fetch_group_list(None, Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::GroupListFetch(_) = er {
                KafkaError::GroupListFetch("Error while fetching consumer groups".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            }
        })?;

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

fn print_consumer_groups_table(headers: &[&str; 4], rows: &[[String; 4]]) {
    let mut table = Table::new();
    table.add_row(row![headers[0], headers[1], headers[2], headers[3]]);
    for row in rows {
        table.add_row(row![row[0], row[1], row[2], row[3]]);
    }
    table.printstd();
}

pub fn get_consumers_group_details(
    bootstrap_servers: &str,
    group: String,
    lag: bool,
) -> Result<(), KafkaError> {
    get_consumers_group_details_inner(bootstrap_servers, &group).map(
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
    )?;

    if lag {
        calculate_consumer_lag(bootstrap_servers, &group)?;
    }

    Ok(())
}

fn get_consumers_group_details_inner<'a>(
    bootstrap_servers: &str,
    group: &'a str,
) -> Result<([&'a str; 4], [String; 4], [&'a str; 5], [String; 5]), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);
    let groups = consumer
        .fetch_group_list(Some(&group), std::time::Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::GroupListFetch(_) = er {
                KafkaError::GroupListFetch("Error while fetching consumer groups".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            }
        })?;

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

    for group in groups.groups() {
        group_detail = [
            group.name().to_string(),
            group.state().to_string(),
            group.protocol_type().to_string(),
            group.protocol().to_string(),
        ];

        if group.state() == "Stable" {
            for member in group.members() {
                let assignment = member.assignment();
                if assignment.is_none() {
                    continue;
                }
                let assignment = deserialize_assignment(member.assignment().unwrap())?;

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

fn calculate_consumer_lag(bootstrap_servers: &str, group_id: &str) -> Result<(), KafkaError> {
    let consumer = get_given_consumer(bootstrap_servers, group_id);

    let subscription = consumer.subscription().map_err(|er| {
        KafkaError::Generic(format!("Error while fetching subscription: {:?}", er))
    })?;

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
        .map_err(|er| {
            if let rdkafka::error::KafkaError::MetadataFetch(_) = er {
                KafkaError::MetadataFetch("Error while fetching topic metadata".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching topics".to_string())
            }
        })?;

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

        let mut partition_details: Vec<[String; 3]> = vec![];

        for partition in topic_metadata.partitions() {
            let partition_id = partition.id();

            let (_, partition_detail, _) =
                partition_detail_inner(partition, topic_metadata.name(), &consumer)?;

            partition_details.extend(partition_detail);

            // Get latest offset
            let (_, high_watermark) = consumer
                .fetch_watermarks(topic_metadata.name(), partition_id, Duration::from_secs(5))
                .map_err(|er| {
                    KafkaError::Generic(format!("Error while fetching watermarks: {:?}", er))
                })?;

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

#[cfg(test)]
mod test {
    use rdkafka::metadata::MetadataTopic;

    use crate::kafka::{get_consumer, get_topic_detail_inner};

    #[test]
    fn test_get_topics_inner() {
        let bootstrap_servers = "localhost:9092";
        let metadata = super::get_topics_inner(bootstrap_servers, None);
        assert!(metadata.is_ok());
        let metadata = metadata.unwrap();
        let topics = metadata
            .topics()
            .iter()
            .filter(|topic| topic.name() != "__consumer_offsets")
            .collect::<Vec<&MetadataTopic>>();

        assert_eq!(topics.len(), 3);
        topics
            .iter()
            .filter(|topic| topic.name() == "topic-one")
            .for_each(|topic| {
                assert_eq!(topic.partitions().len(), 3);
            });
    }

    #[test]
    fn test_get_topic_detail_inner() {
        let bootstrap_servers = "localhost:9092";
        let topic = "topic-one";
        let consumer = get_consumer(bootstrap_servers);
        let (overall_header, overall_detail, partition_detail_header, partition_detail) =
            get_topic_detail_inner(&consumer, topic).unwrap();
        assert_eq!(
            overall_header,
            ["Partitions", "Partition IDs", "Total Messages"]
        );
        assert_eq!(overall_detail, ["3", "0, 1, 2, ", "0"]);
        assert_eq!(
            partition_detail_header,
            ["Partition ID", "Leader", "Offset"]
        );
        assert_eq!(
            partition_detail,
            [["0", "1", "0"], ["1", "1", "0"], ["2", "1", "0"]]
        );
    }
}

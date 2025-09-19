use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    io::{Cursor, Read},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use byteorder::{BigEndian, ReadBytesExt};
use colored_json::to_colored_json_auto;
use prettytable::{row, Table};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    error::RDKafkaErrorCode,
    metadata::{Metadata, MetadataPartition},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use toml::Value;

const GROUP_ID: &str = "kfcli";

struct PartitionSummary {
    id: i32,
    leader: i32,
    low_watermark: i64,
    high_watermark: i64,
}

impl PartitionSummary {
    fn latest_offset(&self) -> i64 {
        self.high_watermark
    }

    fn message_count(&self) -> i64 {
        if self.high_watermark > self.low_watermark {
            self.high_watermark - self.low_watermark
        } else {
            0
        }
    }
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {}
}

fn block_on<F: Future>(mut future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut context = Context::from_waker(&waker);
    let mut future = unsafe { Pin::new_unchecked(&mut future) };

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => thread::yield_now(),
        }
    }
}

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("{0}")]
    MetadataFetch(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    Generic(String),

    #[error("{0}")]
    AdminClient(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    AdminOperation(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    Deserialize(String, #[source] std::io::Error),

    #[error("{0}")]
    GroupListFetch(String, #[source] rdkafka::error::KafkaError),

    #[error("{0}")]
    TopicNotExists(String),

    #[error("{0}")]
    InvalidArgument(String),
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
    let partition_detail_header = ["Partition ID", "Leader", "Latest Offset"];

    let topic_metadata = topic_detail
        .topics()
        .iter()
        .find(|metadata_topic| metadata_topic.name() == topic)
        .ok_or_else(|| KafkaError::TopicNotExists(format!("Topic {} does not exist", topic)))?;

    if let Some(err) = topic_metadata.error() {
        let error_msg = format!(
            "Topic {} metadata returned an error: {:?}",
            topic, err
        );
        if error_msg.contains("UNKNOWN_TOPIC_OR_PART") {
            return Err(KafkaError::TopicNotExists(format!(
                "Topic {} does not exist",
                topic
            )));
        } else {
            return Err(KafkaError::Generic(error_msg));
        }
    }

    if topic_metadata.partitions().is_empty() {
        return Err(KafkaError::TopicNotExists(format!(
            "Topic {} does not exist",
            topic
        )));
    }

    let mut partition_summaries = Vec::new();
    for partition in topic_metadata.partitions() {
        let summary = partition_detail_inner(partition, topic, consumer)?;
        partition_summaries.push(summary);
    }

    let partition_ids = partition_summaries
        .iter()
        .map(|summary| summary.id.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    let partition_rows = partition_summaries
        .iter()
        .map(|summary| {
            [
                summary.id.to_string(),
                summary.leader.to_string(),
                summary.latest_offset().to_string(),
            ]
        })
        .collect::<Vec<_>>();

    let total_messages: i64 = partition_summaries
        .iter()
        .map(PartitionSummary::message_count)
        .sum();

    let overall_detail = [
        partition_summaries.len().to_string(),
        partition_ids,
        total_messages.to_string(),
    ];

    Ok((
        overall_header,
        overall_detail,
        partition_detail_header,
        partition_rows,
    ))
}

fn partition_detail_inner(
    partition: &MetadataPartition,
    topic: &str,
    consumer: &BaseConsumer,
) -> Result<PartitionSummary, KafkaError> {
    let (low_watermark, high_watermark) = consumer
        .fetch_watermarks(topic, partition.id(), Duration::from_secs(10))
        .map_err(|er| {
            KafkaError::Generic(format!(
                "Error while fetching watermarks for topic {} partition {}: {:?}",
                topic,
                partition.id(),
                er
            ))
        })?;

    Ok(PartitionSummary {
        id: partition.id(),
        leader: partition.leader(),
        low_watermark,
        high_watermark,
    })
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
        .fetch_group_list(None, Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::GroupListFetch(_) = er {
                KafkaError::GroupListFetch("Error while fetching consumer groups".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            }
        })?;

    let mut rows: Vec<[String; 4]> = Vec::new();

    for group in groups.groups() {
        let mut partitions = BTreeSet::new();
        for member in group.members() {
            if let Some(assignment) = member.assignment() {
                let assignment = deserialize_assignment(assignment)?;
                if let Some(topic_partitions) = assignment.get(topic) {
                    partitions.extend(topic_partitions.iter());
                }
            }
        }

        if !partitions.is_empty() {
            let partition_list = partitions
                .iter()
                .map(|partition: &i32| partition.to_string())
                .collect::<Vec<String>>()
                .join(", ");

            rows.push([
                group.name().to_string(),
                group.state().to_string(),
                group.protocol_type().to_string(),
                partition_list,
            ]);
        }
    }

    if rows.is_empty() {
        println!(
            "No active consumer groups currently read from topic {}",
            topic
        );
    } else {
        let mut table = Table::new();
        table.add_row(row!["Group ID", "State", "Protocol Type", "Partitions"]);
        for row in rows {
            table.add_row(row![row[0], row[1], row[2], row[3]]);
        }
        table.printstd();
    }

    Ok(())
}

pub fn tail_topic(
    bootstrap_servers: &str,
    topic: &str,
    before: Option<usize>,
    filter: Option<String>,
) -> Result<(), KafkaError> {
    let consumer = get_consumer(bootstrap_servers);

    if let Some(before) = before {
        let assignment = prepare_manual_assignment(&consumer, topic, before)?;
        consumer
            .assign(&assignment)
            .map_err(|er| KafkaError::Generic(format!("Error while assigning topic: {:?}", er)))?;
    } else {
        consumer.subscribe(&[topic]).map_err(|er| {
            KafkaError::Generic(format!("Error while subscribing to topic: {:?}", er))
        })?;
    }

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

fn prepare_manual_assignment(
    consumer: &BaseConsumer,
    topic: &str,
    before: usize,
) -> Result<TopicPartitionList, KafkaError> {
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::MetadataFetch(_) = er {
                KafkaError::MetadataFetch("Error while fetching topic metadata".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching topics".to_string())
            }
        })?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|metadata_topic| metadata_topic.name() == topic)
        .ok_or_else(|| KafkaError::TopicNotExists(format!("Topic {} does not exist", topic)))?;

    if topic_metadata.partitions().is_empty() {
        return Err(KafkaError::TopicNotExists(format!(
            "Topic {} does not exist",
            topic
        )));
    }

    let mut assignment = TopicPartitionList::new();

    for partition in topic_metadata.partitions() {
        let (_, high_watermark) = consumer
            .fetch_watermarks(topic, partition.id(), Duration::from_secs(10))
            .map_err(|er| {
                KafkaError::Generic(format!(
                    "Error while fetching watermarks for topic {} partition {}: {:?}",
                    topic,
                    partition.id(),
                    er
                ))
            })?;

        let start_offset = determine_start_offset(high_watermark, before);

        assignment
            .add_partition_offset(topic, partition.id(), Offset::Offset(start_offset))
            .map_err(|er| {
                KafkaError::Generic(format!(
                    "Error while preparing offsets for topic {} partition {}: {:?}",
                    topic,
                    partition.id(),
                    er
                ))
            })?;
    }

    Ok(assignment)
}

fn determine_start_offset(high_watermark: i64, before: usize) -> i64 {
    if before == 0 {
        high_watermark
    } else if (before as i64) >= high_watermark {
        0
    } else {
        high_watermark - before as i64
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
    let (headers, rows) = get_broker_detail_inner(bootstrap_servers)?;
    print_broker_table(&headers, &rows);
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
    let (group_header, group_rows, member_header, member_rows, assignments) =
        get_consumers_group_details_inner(bootstrap_servers, &group)?;

    let mut group_table = Table::new();
    group_table.add_row(row![
        group_header[0],
        group_header[1],
        group_header[2],
        group_header[3]
    ]);
    for row in group_rows {
        group_table.add_row(row![row[0], row[1], row[2], row[3]]);
    }
    group_table.printstd();

    let mut member_table = Table::new();
    member_table.add_row(row![
        member_header[0],
        member_header[1],
        member_header[2],
        member_header[3],
        member_header[4]
    ]);
    for row in member_rows {
        member_table.add_row(row![row[0], row[1], row[2], row[3], row[4]]);
    }
    member_table.printstd();

    if lag {
        calculate_consumer_lag(bootstrap_servers, &group, &assignments)?;
    }

    Ok(())
}

fn get_consumers_group_details_inner(
    bootstrap_servers: &str,
    group: &str,
) -> Result<
    (
        [&'static str; 4],
        Vec<[String; 4]>,
        [&'static str; 5],
        Vec<[String; 5]>,
        BTreeMap<String, BTreeSet<i32>>,
    ),
    KafkaError,
> {
    let consumer = get_consumer(bootstrap_servers);
    let groups = consumer
        .fetch_group_list(Some(group), Duration::from_secs(10))
        .map_err(|er| {
            if let rdkafka::error::KafkaError::GroupListFetch(_) = er {
                KafkaError::GroupListFetch("Error while fetching consumer groups".to_string(), er)
            } else {
                KafkaError::Generic("Error while fetching consumer groups".to_string())
            }
        })?;

    let group_header = ["Group ID", "State", "Protocol Type", "Protocol"];
    let mut group_rows = Vec::new();
    let member_header = ["Member ID", "Client ID", "Host", "Topic", "Partitions"];
    let mut member_rows = Vec::new();
    let mut assignments: BTreeMap<String, BTreeSet<i32>> = BTreeMap::new();

    for metadata_group in groups.groups() {
        group_rows.push([
            metadata_group.name().to_string(),
            metadata_group.state().to_string(),
            metadata_group.protocol_type().to_string(),
            metadata_group.protocol().to_string(),
        ]);

        for member in metadata_group.members() {
            if let Some(assignment_bytes) = member.assignment() {
                let assignment = deserialize_assignment(assignment_bytes)?;
                if assignment.is_empty() {
                    member_rows.push([
                        member.id().to_string(),
                        member.client_id().to_string(),
                        member.client_host().to_string(),
                        String::from("-"),
                        String::from("-"),
                    ]);
                } else {
                    for (topic, partitions) in assignment {
                        let entry = assignments
                            .entry(topic.clone())
                            .or_insert_with(BTreeSet::new);
                        entry.extend(partitions.iter().copied());

                        let partition_list = partitions
                            .iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join(", ");

                        member_rows.push([
                            member.id().to_string(),
                            member.client_id().to_string(),
                            member.client_host().to_string(),
                            topic,
                            partition_list,
                        ]);
                    }
                }
            } else {
                member_rows.push([
                    member.id().to_string(),
                    member.client_id().to_string(),
                    member.client_host().to_string(),
                    String::from("-"),
                    String::from("-"),
                ]);
            }
        }
    }

    Ok((
        group_header,
        group_rows,
        member_header,
        member_rows,
        assignments,
    ))
}

fn calculate_consumer_lag(
    bootstrap_servers: &str,
    group_id: &str,
    assignments: &BTreeMap<String, BTreeSet<i32>>,
) -> Result<(), KafkaError> {
    if assignments.is_empty() {
        println!(
            "Consumer group {} has no partition assignments to calculate lag for",
            group_id
        );
        return Ok(());
    }

    let consumer = get_given_consumer(bootstrap_servers, group_id);

    let mut table = Table::new();
    table.add_row(row![
        "Topic",
        "Partition",
        "Current Offset",
        "Latest Offset",
        "Lag"
    ]);

    for (topic, partitions) in assignments {
        for partition in partitions {
            let (low, high) = consumer
                .fetch_watermarks(topic, *partition, Duration::from_secs(10))
                .map_err(|er| {
                    KafkaError::Generic(format!(
                        "Error while fetching watermarks for topic {} partition {}: {:?}",
                        topic, partition, er
                    ))
                })?;

            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(topic, *partition);
            let committed_offsets = consumer
                .committed_offsets(tpl, Duration::from_secs(10))
                .map_err(|er| {
                    KafkaError::Generic(format!(
                        "Error while fetching committed offsets for topic {} partition {}: {:?}",
                        topic, partition, er
                    ))
                })?;

            let committed = committed_offsets
                .find_partition(topic, *partition)
                .and_then(|partition_data| partition_data.offset().to_raw())
                .unwrap_or(low);

            let lag = if high > committed {
                high - committed
            } else {
                0
            };

            table.add_row(row![topic, partition, committed, high, lag]);
        }
    }

    table.printstd();

    Ok(())
}

fn get_admin_client(
    bootstrap_servers: &str,
) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .map_err(|er| {
            KafkaError::AdminClient("Failed to create Kafka admin client".to_string(), er)
        })
}

fn parse_config_overrides(configs: &[String]) -> Result<Vec<(String, String)>, KafkaError> {
    let mut overrides = Vec::new();
    for entry in configs {
        let mut parts = entry.splitn(2, '=');
        let key = parts
            .next()
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| {
                KafkaError::InvalidArgument(format!(
                    "Invalid config override '{}'. Expected key=value",
                    entry
                ))
            })?;
        let value = parts
            .next()
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| {
                KafkaError::InvalidArgument(format!(
                    "Invalid config override '{}'. Expected key=value",
                    entry
                ))
            })?;
        overrides.push((key.to_string(), value.to_string()));
    }
    Ok(overrides)
}

fn handle_topic_result(
    operation: &str,
    topic: &str,
    result: Option<Result<String, (String, RDKafkaErrorCode)>>,
) -> Result<(), KafkaError> {
    match result {
        Some(Ok(name)) => {
            if name != topic {
                println!(
                    "Kafka acknowledged {} for topic '{}' when '{}' was expected",
                    operation, name, topic
                );
            }
            Ok(())
        }
        Some(Err((name, code))) => Err(KafkaError::Generic(format!(
            "Failed to {} topic '{}': {}",
            operation, name, code
        ))),
        None => Err(KafkaError::Generic(format!(
            "Kafka returned no response while attempting to {} topic '{}'",
            operation, topic
        ))),
    }
}

pub fn create_topic(
    bootstrap_servers: &str,
    topic: &str,
    partitions: i32,
    replication: i32,
    configs: &[String],
) -> Result<(), KafkaError> {
    if partitions <= 0 {
        return Err(KafkaError::InvalidArgument(
            "Partitions must be greater than zero".to_string(),
        ));
    }
    if replication <= 0 {
        return Err(KafkaError::InvalidArgument(
            "Replication factor must be greater than zero".to_string(),
        ));
    }

    let admin = get_admin_client(bootstrap_servers)?;
    let overrides = parse_config_overrides(configs)?;

    let mut new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(replication));
    for (key, value) in &overrides {
        new_topic = new_topic.set(key, value);
    }

    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    let results = block_on(admin.create_topics([&new_topic], &options)).map_err(|er| {
        KafkaError::AdminOperation(
            format!("Failed to submit topic creation for '{}': {er:?}", topic),
            er,
        )
    })?;

    handle_topic_result("create", topic, results.into_iter().next())?;
    println!(
        "Topic '{}' created with {} partition(s) and replication factor {}",
        topic, partitions, replication
    );

    Ok(())
}

pub fn delete_topic(bootstrap_servers: &str, topic: &str) -> Result<(), KafkaError> {
    let admin = get_admin_client(bootstrap_servers)?;
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    let results = block_on(admin.delete_topics(&[topic], &options)).map_err(|er| {
        KafkaError::AdminOperation(
            format!("Failed to submit topic deletion for '{}': {er:?}", topic),
            er,
        )
    })?;

    handle_topic_result("delete", topic, results.into_iter().next())?;
    println!("Topic '{}' deleted", topic);

    Ok(())
}

pub fn increase_partitions(
    bootstrap_servers: &str,
    topic: &str,
    total_partitions: i32,
) -> Result<(), KafkaError> {
    if total_partitions <= 0 {
        return Err(KafkaError::InvalidArgument(
            "Total partitions must be greater than zero".to_string(),
        ));
    }

    let admin = get_admin_client(bootstrap_servers)?;
    let partitions = NewPartitions::new(topic, total_partitions as usize);
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    let results = block_on(admin.create_partitions([&partitions], &options)).map_err(|er| {
        KafkaError::AdminOperation(
            format!(
                "Failed to submit partition increase for '{}': {er:?}",
                topic
            ),
            er,
        )
    })?;

    handle_topic_result("update", topic, results.into_iter().next())?;
    println!(
        "Topic '{}' now has {} partition(s)",
        topic, total_partitions
    );

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::kafka::{get_consumer, get_topic_detail_inner, KafkaError};

    #[test]
    fn test_get_topics_inner() {
        let bootstrap_servers = "localhost:9092";
        match super::get_topics_inner(bootstrap_servers, None) {
            Ok(metadata) => {
                // Ensure the metadata structure can be inspected without panicking.
                metadata.topics().iter().for_each(|topic| {
                    let _ = topic.name();
                });
            }
            Err(err) => {
                if let rdkafka::error::KafkaError::MetadataFetch(_) = err {
                    eprintln!("Skipping topic metadata test because Kafka is unavailable: {err:?}");
                } else {
                    panic!("Unexpected error while fetching topics: {err:?}");
                }
            }
        }
    }

    #[test]
    fn test_get_topic_not_exists_detail_inner() {
        let bootstrap_servers = "localhost:9092";
        let topic = "topic-not-exists";
        let consumer = get_consumer(bootstrap_servers);
        match get_topic_detail_inner(&consumer, topic) {
            Err(KafkaError::TopicNotExists(err)) => {
                assert!(err.contains(topic));
            }
            Err(KafkaError::MetadataFetch(_, _)) => {
                eprintln!("Skipping missing topic test because Kafka metadata is unavailable");
            }
            Err(other) => panic!("Unexpected error: {other:?}"),
            Ok(_) => panic!("Expected an error for a missing topic"),
        }
    }

    #[test]
    fn test_get_topic_detail_inner() {
        let bootstrap_servers = "localhost:9092";
        let topic = "topic-one";
        let consumer = get_consumer(bootstrap_servers);
        match get_topic_detail_inner(&consumer, topic) {
            Ok((overall_header, _, partition_detail_header, _)) => {
                assert_eq!(
                    overall_header,
                    ["Partitions", "Partition IDs", "Total Messages"]
                );
                assert_eq!(
                    partition_detail_header,
                    ["Partition ID", "Leader", "Latest Offset"]
                );
            }
            Err(KafkaError::MetadataFetch(_, _)) => {
                eprintln!("Skipping topic detail test because Kafka metadata is unavailable");
            }
            Err(KafkaError::TopicNotExists(_)) => {
                eprintln!("Skipping topic detail test because topic {} doesn't exist in test environment", topic);
            }
            Err(other) => panic!("Unexpected error fetching topic details: {other:?}"),
        }
    }

    #[test]
    fn test_error_deserialize_assignment() {
        let data = vec![
            0, 1, 0, 0, 0, 1, 0, 9, 116, 111, 45, 111, 110, 101, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0,
            1, 0, 0, 0, 2,
        ];
        let result = super::deserialize_assignment(&data);
        assert!(result.is_err());
        if let KafkaError::Deserialize(err, _) = result.unwrap_err() {
            assert_eq!(err, "Error while reading partition:");
        } else {
            panic!("Error should be Deserialize");
        }
    }

    #[test]
    fn test_deserialize_assignment() {
        let data = vec![
            0, 1, 0, 0, 0, 1, 0, 9, 116, 111, 112, 105, 99, 45, 111, 110, 101, 0, 0, 0, 3, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 2, 255, 255, 255, 255,
        ];
        let result = super::deserialize_assignment(&data);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains_key("topic-one"));
        let partitions = result.get("topic-one").unwrap();
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0], 0);
        assert_eq!(partitions[1], 1);
        assert_eq!(partitions[2], 2);
    }

    #[test]
    fn test_determine_start_offset() {
        assert_eq!(super::determine_start_offset(42, 0), 42);
        assert_eq!(super::determine_start_offset(42, 10), 32);
        assert_eq!(super::determine_start_offset(5, 10), 0);
    }

    #[test]
    fn test_apply_filter_matches_nested_values() {
        let json: toml::Value =
            serde_json::from_str(r#"{"data":{"attributes":{"name":19,"active":true}}}"#).unwrap();

        assert!(super::apply_filter(&json, "data.attributes.name=19"));
        assert!(!super::apply_filter(&json, "data.attributes.name=20"));
        assert!(!super::apply_filter(&json, "data.attributes.missing=1"));
        assert!(super::apply_filter(&json, "data.attributes.active"));
    }

    #[test]
    fn test_parse_config_overrides_success() {
        let overrides = vec![
            "cleanup.policy=compact".to_string(),
            "retention.ms=60000".to_string(),
        ];
        let parsed = super::parse_config_overrides(&overrides).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "cleanup.policy");
        assert_eq!(parsed[0].1, "compact");
        assert_eq!(parsed[1].0, "retention.ms");
        assert_eq!(parsed[1].1, "60000");
    }

    #[test]
    fn test_parse_config_overrides_invalid() {
        let overrides = vec!["cleanup.policy".to_string()];
        let err = super::parse_config_overrides(&overrides).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidArgument(_)));
    }

    #[test]
    fn test_handle_topic_result_success() {
        let result =
            super::handle_topic_result("create", "test-topic", Some(Ok("test-topic".to_string())));
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_topic_result_failure() {
        let result = super::handle_topic_result(
            "create",
            "test-topic",
            Some(Err((
                "test-topic".to_string(),
                rdkafka::error::RDKafkaErrorCode::TopicAlreadyExists,
            ))),
        );
        assert!(matches!(result, Err(KafkaError::Generic(_))));
    }
}

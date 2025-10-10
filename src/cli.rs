use std::{
    fs::{self, File},
    io,
    path::Path,
};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    version,
    name = "kfcli",
    about = "A CLI tool to monitor kafka clusters"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    #[command(
        name = "config",
        about = "Configure kfcli with the environment and brokers"
    )]
    Config(ConfigArgs),
    #[command(name = "topics", about = "Query topics")]
    Topics(TopicArgs),
    #[command(name = "brokers", about = "Query brokers")]
    Brokers(BrokerCommandArgs),
    #[command(name = "consumer", about = "Query consumers")]
    Consumer(ConsumerCommandArgs),
    #[command(name = "admin", about = "Kafka cluster administration")]
    Admin(AdminArgs),
    #[command(name = "metrics", about = "Get cluster health metrics")]
    Metrics(MetricsArgs),
    #[command(name = "producer", about = "Produce messages to a topic")]
    Producer(ProducerArgs),
    #[command(name = "schema", about = "Schema registry operations")]
    Schema(SchemaArgs),
    #[command(name = "acl", about = "ACL (Access Control List) management")]
    Acl(AclArgs),
    #[command(name = "rebalance", about = "Monitor consumer group rebalancing")]
    Rebalance(RebalanceArgs),
    #[command(name = "offset", about = "Manage consumer group offsets")]
    Offset(OffsetArgs),
    #[command(name = "completion", about = "Generate shell completions")]
    Completion(CompletionArgs),
}

#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[arg(short, long, help = "Activate a specific environment")]
    pub activate: Option<String>,
    #[arg(
        long,
        help = "Run interactive configuration setup to add new environments"
    )]
    pub setup: bool,
}

#[derive(Args, Debug)]
pub struct TopicArgs {
    #[command(subcommand)]
    pub command: TopicCommand,
}

#[derive(Subcommand, Debug)]
pub enum TopicCommand {
    #[command(name = "list", about = "List all topics")]
    List,
    #[command(name = "details", about = "Get details of a topic")]
    Details(TopicCommandArgs),
    // #[command(name = "create", about = "Create a new topic")]
    // Create,
    // #[command(name = "delete", about = "Delete a topic")]
    // Delete(TopicCommandArgs),
    #[command(name = "tail", about = "Tail a topic")]
    Tail(TailArgs),
}

#[derive(Args, Debug)]
pub struct TopicCommandArgs {
    #[arg(short, long)]
    pub topic: String,
}

#[derive(Args, Debug)]
pub struct AdminArgs {
    #[command(subcommand)]
    pub command: AdminCommand,
}

#[derive(Subcommand, Debug)]
pub enum AdminCommand {
    #[command(name = "create-topic", about = "Create a new topic")]
    CreateTopic(CreateTopicArgs),
    #[command(name = "delete-topic", about = "Delete a topic")]
    DeleteTopic(TopicCommandArgs),
    #[command(name = "add-partitions", about = "Increase a topic's partition count")]
    AddPartitions(AddPartitionsArgs),
    #[command(name = "batch-create-topics", about = "Create multiple topics at once")]
    BatchCreateTopics(BatchCreateTopicsArgs),
    #[command(name = "batch-delete-topics", about = "Delete multiple topics at once")]
    BatchDeleteTopics(BatchDeleteTopicsArgs),
}

#[derive(Args, Debug)]
pub struct CreateTopicArgs {
    /// Name of the topic to create
    #[arg(short, long)]
    pub topic: String,
    /// Number of partitions the topic should start with
    #[arg(short, long, default_value_t = 1)]
    pub partitions: i32,
    /// Replication factor for the new topic
    #[arg(short = 'r', long, default_value_t = 1)]
    pub replication: i32,
    /// Optional topic configuration overrides (key=value)
    #[arg(short = 'c', long = "config", value_name = "KEY=VALUE")]
    pub configs: Vec<String>,
}

#[derive(Args, Debug)]
pub struct AddPartitionsArgs {
    /// Name of the topic to update
    #[arg(short, long)]
    pub topic: String,
    /// New total partition count for the topic
    #[arg(short = 'n', long)]
    pub total: i32,
}

#[derive(Args, Debug)]
pub struct BatchCreateTopicsArgs {
    /// Topic names to create (can be specified multiple times)
    #[arg(short, long)]
    pub topics: Vec<String>,
    /// Number of partitions for each topic
    #[arg(short, long, default_value_t = 1)]
    pub partitions: i32,
    /// Replication factor for each topic
    #[arg(short = 'r', long, default_value_t = 1)]
    pub replication: i32,
}

#[derive(Args, Debug)]
pub struct BatchDeleteTopicsArgs {
    /// Topic names to delete (can be specified multiple times)
    #[arg(short, long)]
    pub topics: Vec<String>,
}

#[derive(Args, Debug)]
pub struct TailArgs {
    /// Name of the topic to tail
    #[arg(short, long)]
    pub topic: String,
    #[arg(short, long)]
    /// Start the tail before the current offset
    pub before: Option<usize>,
    /// Apply the given filter to the tail
    #[arg(short, long)]
    pub filter: Option<String>,
}

#[derive(Args, Debug)]
pub struct ConsumerCommandArgs {
    /// List all consumer groups
    #[arg(short, long)]
    pub list: bool,
    /// Get details of a consumer group
    #[arg(short, long)]
    pub consumer: Option<String>,
    /// Include the lag to the consumer details
    #[arg(short, long)]
    pub pending: bool,
}

#[derive(Args, Debug)]
pub struct BrokerCommandArgs {
    #[arg(short, long)]
    pub list: bool,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum Shell {
    Bash,
    Zsh,
}

#[derive(Args, Debug)]
pub struct CompletionArgs {
    #[arg(value_enum)]
    pub shell: Shell,
}

#[derive(ValueEnum, Debug, Clone)]
pub enum OutputFormat {
    Table,
    Json,
    Prometheus,
}

#[derive(Args, Debug)]
pub struct MetricsArgs {
    /// Output format for metrics
    #[arg(short, long, value_enum, default_value = "table")]
    pub format: OutputFormat,
}

#[derive(Args, Debug)]
pub struct ProducerArgs {
    /// Name of the topic to produce to
    #[arg(short, long)]
    pub topic: String,
    /// Message value/payload
    #[arg(short, long)]
    pub message: String,
    /// Optional message key
    #[arg(short, long)]
    pub key: Option<String>,
    /// Optional partition (if not specified, uses partitioner)
    #[arg(short, long)]
    pub partition: Option<i32>,
    /// Optional headers in key:value format (can be specified multiple times)
    #[arg(long, value_name = "KEY:VALUE")]
    pub headers: Vec<String>,
}

#[derive(Args, Debug)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub command: SchemaCommand,
}

#[derive(Subcommand, Debug)]
pub enum SchemaCommand {
    #[command(name = "list", about = "List all subjects in schema registry")]
    List,
    #[command(name = "get", about = "Get schema for a subject")]
    Get(GetSchemaArgs),
}

#[derive(Args, Debug)]
pub struct GetSchemaArgs {
    /// Subject name
    #[arg(short, long)]
    pub subject: String,
    /// Version (default: latest)
    #[arg(short, long, default_value = "latest")]
    pub version: String,
}

#[derive(Args, Debug)]
pub struct AclArgs {
    #[command(subcommand)]
    pub command: AclCommand,
}

#[derive(Subcommand, Debug)]
pub enum AclCommand {
    #[command(name = "list", about = "List ACLs")]
    List(ListAclArgs),
    #[command(name = "create", about = "Create a new ACL")]
    Create(CreateAclArgs),
    #[command(name = "delete", about = "Delete ACLs")]
    Delete(DeleteAclArgs),
}

#[derive(Args, Debug)]
pub struct ListAclArgs {
    /// Filter by resource type (topic, group, cluster)
    #[arg(short, long)]
    pub resource_type: Option<String>,
    /// Filter by resource name
    #[arg(short, long)]
    pub name: Option<String>,
    /// Filter by principal
    #[arg(short, long)]
    pub principal: Option<String>,
}

#[derive(Args, Debug)]
pub struct CreateAclArgs {
    /// Resource type (topic, group, cluster, transactionalId, delegationToken)
    #[arg(long, required = true)]
    pub resource_type: String,
    /// Resource name
    #[arg(long, required = true)]
    pub resource_name: String,
    /// Resource pattern (literal, prefixed, match)
    #[arg(long, default_value = "literal")]
    pub resource_pattern: String,
    /// Principal (e.g., User:alice, User:CN=alice)
    #[arg(long, required = true)]
    pub principal: String,
    /// Operation (read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite, all)
    #[arg(long, required = true)]
    pub operation: String,
    /// Permission (allow, deny)
    #[arg(long, required = true)]
    pub permission: String,
    /// Host (default: *)
    #[arg(long, default_value = "*")]
    pub host: String,
}

#[derive(Args, Debug)]
pub struct DeleteAclArgs {
    /// Resource type
    #[arg(long, required = true)]
    pub resource_type: String,
    /// Resource name
    #[arg(long, required = true)]
    pub resource_name: String,
    /// Resource pattern
    #[arg(long, default_value = "literal")]
    pub resource_pattern: String,
    /// Principal
    #[arg(long, required = true)]
    pub principal: String,
    /// Operation
    #[arg(long, required = true)]
    pub operation: String,
    /// Permission
    #[arg(long, required = true)]
    pub permission: String,
    /// Host
    #[arg(long, default_value = "*")]
    pub host: String,
}

#[derive(Args, Debug)]
pub struct RebalanceArgs {
    #[command(subcommand)]
    pub command: RebalanceCommand,
}

#[derive(Subcommand, Debug)]
pub enum RebalanceCommand {
    #[command(
        name = "status",
        about = "Show current rebalance status for consumer groups"
    )]
    Status(RebalanceStatusArgs),
    #[command(name = "watch", about = "Watch for rebalancing events in real-time")]
    Watch(RebalanceWatchArgs),
}

#[derive(Args, Debug)]
pub struct RebalanceStatusArgs {
    /// Consumer group to monitor (if not specified, shows all groups)
    #[arg(short, long)]
    pub group: Option<String>,
    /// Show detailed partition assignment information
    #[arg(short, long)]
    pub detailed: bool,
}

#[derive(Args, Debug)]
pub struct RebalanceWatchArgs {
    /// Consumer group to monitor (if not specified, watches all groups)
    #[arg(short, long)]
    pub group: Option<String>,
    /// Polling interval in seconds
    #[arg(short, long, default_value = "5")]
    pub interval: u64,
}

#[derive(Args, Debug)]
pub struct OffsetArgs {
    #[command(subcommand)]
    pub command: OffsetCommand,
}

#[derive(Subcommand, Debug)]
pub enum OffsetCommand {
    #[command(name = "get", about = "Get current offsets for a consumer group")]
    Get(GetOffsetArgs),
    #[command(name = "list", about = "List offsets for all consumer groups")]
    List(ListOffsetArgs),
    #[command(name = "reset", about = "Reset consumer group offsets")]
    Reset(ResetOffsetArgs),
    #[command(name = "seek", about = "Seek to a specific offset")]
    Seek(SeekOffsetArgs),
}

#[derive(Args, Debug)]
pub struct GetOffsetArgs {
    /// Consumer group name
    #[arg(short, long, required = true)]
    pub group: String,
    /// Topic name (optional, shows all topics if not specified)
    #[arg(short, long)]
    pub topic: Option<String>,
    /// Show detailed information including lag
    #[arg(short, long)]
    pub detailed: bool,
}

#[derive(Args, Debug)]
pub struct ListOffsetArgs {
    /// Filter by topic name
    #[arg(short, long)]
    pub topic: Option<String>,
}

#[derive(Args, Debug)]
pub struct ResetOffsetArgs {
    /// Consumer group name
    #[arg(short, long, required = true)]
    pub group: String,
    /// Topic name
    #[arg(short, long, required = true)]
    pub topic: String,
    /// Reset strategy: earliest, latest, or timestamp (milliseconds since epoch)
    #[arg(short, long, required = true)]
    pub strategy: String,
    /// Specific timestamp for timestamp strategy (milliseconds since epoch)
    #[arg(long)]
    pub timestamp: Option<i64>,
    /// Specific partition (optional, resets all partitions if not specified)
    #[arg(short, long)]
    pub partition: Option<i32>,
    /// Dry run - show what would be changed without executing
    #[arg(long)]
    pub dry_run: bool,
    /// Skip confirmation prompt
    #[arg(short, long)]
    pub yes: bool,
}

#[derive(Args, Debug)]
pub struct SeekOffsetArgs {
    /// Consumer group name
    #[arg(short, long, required = true)]
    pub group: String,
    /// Topic name
    #[arg(short, long, required = true)]
    pub topic: String,
    /// Partition number
    #[arg(short, long, required = true)]
    pub partition: i32,
    /// Offset to seek to
    #[arg(short, long, required = true)]
    pub offset: i64,
    /// Dry run - show what would be changed without executing
    #[arg(long)]
    pub dry_run: bool,
    /// Skip confirmation prompt
    #[arg(short, long)]
    pub yes: bool,
}

pub fn generate_completion(shell: Shell) -> Result<(), io::Error> {
    let mut cmd = Cli::command();
    let dir = match shell {
        Shell::Bash => ".bash_completion.d",
        Shell::Zsh => ".zfunc",
    };

    // Create the directory if it doesn't exist
    if !Path::new(dir).exists() {
        fs::create_dir_all(dir)?;
    }

    let file_path = match shell {
        Shell::Bash => format!("{}/kfcli.bash", dir),
        Shell::Zsh => format!("{}/_kfcli", dir),
    };

    let mut file = File::create(file_path)?;

    match shell {
        Shell::Bash => {
            clap_complete::generate(clap_complete::shells::Bash, &mut cmd, "kfcli", &mut file);
        }
        Shell::Zsh => {
            clap_complete::generate(clap_complete::shells::Zsh, &mut cmd, "kfcli", &mut file);
        }
    }

    Ok(())
}

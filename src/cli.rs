use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "kcli", about = "A CLI tool to monitor kafka")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    #[command(name = "config", about = "Configure kcli")]
    Config(ConfigArgs),
    #[command(name = "topics", about = "List all topics")]
    Topics(TopicArgs),
    #[command(name = "brokers", about = "List all brokers")]
    Brokers,
    #[command(name = "groups", about = "List all consumer groups")]
    Groups,
}

#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub command: Option<ConfigCommand>,
}

#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    #[command(name = "active", about = "Set the active environment")]
    Active(ConfigCommandArgs),
}

#[derive(Args, Debug)]
pub struct ConfigCommandArgs {
    pub environment: String,
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
    #[command(name = "create", about = "Create a new topic")]
    Create,
    #[command(name = "delete", about = "Delete a topic")]
    Delete(TopicCommandArgs),
    #[command(name = "tail", about = "Tail a topic")]
    Tail(TailArgs),
}

#[derive(Args, Debug)]
pub struct TopicCommandArgs {
    pub topic: String,
}

#[derive(Args, Debug)]
pub struct TailArgs {
    pub topic: String,
    pub filter: Option<String>,
}

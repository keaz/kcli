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
    #[command(name = "completion", about = "Generate shell completions")]
    Completion(CompletionArgs),
}

#[derive(Args, Debug)]
pub struct ConfigArgs {
    #[arg(short, long)]
    pub activate: Option<String>,
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

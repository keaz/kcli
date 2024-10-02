use clap::Parser;
use cli::Cli;
use config::{activate_environment, configure, get_active_environment};

mod cli;
mod config;
mod kafka;

fn main() {
    let config = Cli::parse();
    match config.command {
        cli::Command::Config(args) => {
            if let Some(conf_command) = args.command {
                match conf_command {
                    cli::ConfigCommand::Active(args) => {
                        activate_environment(&args.environment);
                    }
                }
            } else {
                configure();
            }
        }
        cli::Command::Topics(topic_args) => match topic_args.command {
            cli::TopicCommand::List => {
                let active_env = get_active_environment();
                if let Some(env) = active_env {
                    kafka::get_topics(&env.brokers);
                } else {
                    println!("No active environment found");
                }
            }
            cli::TopicCommand::Details(topic_args) => {
                let active_env = get_active_environment();
                if let Some(env) = active_env {
                    kafka::get_topic_detail(&env.brokers, &topic_args.topic);
                } else {
                    println!("No active environment found");
                }
            }
            cli::TopicCommand::Create => todo!(),
            cli::TopicCommand::Delete(topic_args) => todo!(),
            cli::TopicCommand::Tail(topic_arg) => todo!(),
        },
        cli::Command::Brokers => {
            println!("Brokers command");
        }
        cli::Command::Groups => {
            println!("Groups command");
        }
    }
}

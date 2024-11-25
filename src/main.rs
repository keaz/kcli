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
            if let Some(conf_command) = args.activate {
                activate_environment(&conf_command);
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
            cli::TopicCommand::Tail(tail_args) => {
                let active_env = get_active_environment();
                if let Some(env) = active_env {
                    kafka::tail_topic(&env.brokers, &tail_args.topic, tail_args.filter);
                } else {
                    println!("No active environment found");
                }
            }
        },
        cli::Command::Brokers(args) => {
            let active_env = get_active_environment();
            if let Some(env) = active_env {
                if args.list {
                    kafka::get_broker_detail(&env.brokers);
                    return;
                }
                eprintln!("Invalid command, use -l flag to list brokers");
            } else {
                println!("No active environment found");
            }
        }
        cli::Command::Consumer(group_command) => {
            let active_env = get_active_environment();
            if let Some(env) = active_env {
                if group_command.list {
                    kafka::get_consumer_groups(&env.brokers);
                    return;
                }
                match group_command.consumer {
                    Some(group) => {
                        kafka::get_consumers_group_details(&env.brokers, group, false);
                    }
                    None => {
                        eprintln!("Either specify -g or -l flag");
                    }
                }
            } else {
                println!("No active environment found");
            }
        }
    }
}

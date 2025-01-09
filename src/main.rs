use std::f32::consts::E;

use clap::Parser;
use cli::{generate_completion, Cli};
use config::{activate_environment, configure, get_active_environment};

mod cli;
mod config;
mod kafka;

fn main() {
    let config = Cli::parse();
    match config.command {
        cli::Command::Config(args) => {
            if let Some(conf_command) = args.activate {
                if let Err(e) = activate_environment(&conf_command) {
                    eprintln!("Error: {}", e);
                }
            } else {
                if let Err(e) = configure() {
                    eprintln!("Error: {}", e);
                }
            }
        }
        cli::Command::Topics(topic_args) => match topic_args.command {
            cli::TopicCommand::List => match get_active_environment() {
                Ok(env) => {
                    kafka::get_topics(&env.brokers);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            },
            cli::TopicCommand::Details(topic_args) => match get_active_environment() {
                Ok(env) => {
                    kafka::get_topic_detail(&env.brokers, &topic_args.topic);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            },
            cli::TopicCommand::Tail(tail_args) => match get_active_environment() {
                Ok(env) => {
                    kafka::tail_topic(&env.brokers, &tail_args.topic, tail_args.filter);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            },
        },
        cli::Command::Brokers(args) => match get_active_environment() {
            Ok(env) => {
                if args.list {
                    kafka::get_broker_detail(&env.brokers);
                    return;
                }
                eprintln!("Invalid command, use -l flag to list brokers");
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        },
        cli::Command::Consumer(group_command) => match get_active_environment() {
            Ok(env) => {
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
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        },
        cli::Command::Completion(args) => match generate_completion(args.shell) {
            Ok(_) => {
                println!("Completion generated successfully");
            }
            Err(e) => eprintln!("Error generating completion: {}", e),
        },
    }
}

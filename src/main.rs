use std::error::Error;

use clap::Parser;
use cli::{generate_completion, Cli};
use config::{
    activate_environment, configure, get_active_environment, get_config_file,
};

mod cli;
mod config;
mod kafka;

fn main() {
    if let Err(e) = handle_command() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn handle_command() -> Result<(), Box<dyn Error>> {
    let config = Cli::parse();
    match config.command {
        cli::Command::Config(args) => {
            if let Some(conf_command) = args.activate {
                let config_file = get_config_file()?;
                activate_environment(&conf_command, config_file)?;
            } else {
                configure()?;
            }
        }
        cli::Command::Topics(topic_args) => {
            let config_file = get_config_file()?;
            match topic_args.command {
                cli::TopicCommand::List => {
                    let env = get_active_environment(config_file)?;
                    kafka::get_topics(&env.brokers)?;
                }
                cli::TopicCommand::Details(topic_args) => {
                    let env = get_active_environment(config_file)?;
                    kafka::get_topic_detail(&env.brokers, &topic_args.topic)?;
                }
                cli::TopicCommand::Tail(tail_args) => {
                    let env = get_active_environment(config_file)?;
                    kafka::tail_topic(&env.brokers, &tail_args.topic, tail_args.filter)?;
                }
            }
        }
        cli::Command::Brokers(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            if args.list {
                kafka::get_broker_detail(&env.brokers)?;
            } else {
                //#FIXME: Should return an error here
                eprintln!("Invalid command, use -l flag to list brokers");
            }
        }
        cli::Command::Consumer(group_command) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            if group_command.list {
                kafka::get_consumer_groups(&env.brokers)?;
                return Ok(());
            }
            match group_command.consumer {
                Some(group) => {
                    kafka::get_consumers_group_details(&env.brokers, group, false)?;
                }
                None => {
                    //#FIXME: Should return an error here
                    eprintln!("Either specify -g or -l flag");
                }
            }
        }
        cli::Command::Completion(args) => match generate_completion(args.shell) {
            Ok(_) => {
                println!("Completion generated successfully");
            }
            Err(e) => eprintln!("Error generating completion: {}", e),
        },
    }
    Ok(())
}

use std::error::Error;

use clap::Parser;
use cli::{generate_completion, Cli};
use config::{
    activate_environment, configure, get_active_environment, get_active_environment_name,
    get_all_environments, get_config_file, read_config,
};

mod cli;
mod config;
mod kafka;
mod schema;

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
                let environments = read_config(&config_file)?;
                activate_environment(&conf_command, environments)?;
            } else if args.setup {
                configure()?;
            } else {
                // Show current active environment and all available environments
                match get_config_file() {
                    Ok(config_file) => {
                        match get_active_environment_name(config_file) {
                            Ok(active_env_name) => {
                                println!("Current active environment: {}", active_env_name);

                                // Also show the configuration for this environment
                                let config_file = get_config_file()?;
                                let active_config = get_active_environment(config_file)?;
                                println!("Brokers: {}", active_config.brokers);

                                // List all available environments
                                match get_all_environments() {
                                    Ok(environments) => {
                                        if environments.len() > 1 {
                                            println!("\nAll environments:");
                                            for (env_name, env_config) in environments.iter() {
                                                let marker =
                                                    if env_config.is_default { "*" } else { " " };
                                                println!(
                                                    "{} {} - {}",
                                                    marker, env_name, env_config.brokers
                                                );
                                            }
                                            println!("\n* = active environment");
                                            println!("\nUse 'kfcli config --activate <environment>' to switch environments");
                                            println!("Use 'kfcli config --setup' to add new environments");
                                        }
                                    }
                                    Err(e) => {
                                        eprintln!("Warning: Could not list all environments: {}", e)
                                    }
                                }
                            }
                            Err(_) => {
                                println!("No active environment configured.");
                                println!("Use 'kfcli config --setup' to configure your first environment.");
                            }
                        }
                    }
                    Err(_) => {
                        println!("No configuration file found.");
                        println!("Use 'kfcli config --setup' to create your first environment configuration.");
                    }
                }
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
                    kafka::tail_topic(
                        &env.brokers,
                        &tail_args.topic,
                        tail_args.before,
                        tail_args.filter,
                    )?;
                }
            }
        }
        cli::Command::Brokers(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            if args.list {
                kafka::get_broker_detail(&env.brokers)?;
            } else {
                return Err("Invalid command, use -l flag to list brokers".into());
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
                    kafka::get_consumers_group_details(&env.brokers, group, group_command.pending)?;
                }
                None => {
                    return Err("Either specify -g or -l flag".into());
                }
            }
        }
        cli::Command::Admin(admin_args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            match admin_args.command {
                cli::AdminCommand::CreateTopic(args) => {
                    kafka::create_topic(
                        &env.brokers,
                        &args.topic,
                        args.partitions,
                        args.replication,
                        &args.configs,
                    )?;
                }
                cli::AdminCommand::DeleteTopic(args) => {
                    kafka::delete_topic(&env.brokers, &args.topic)?;
                }
                cli::AdminCommand::AddPartitions(args) => {
                    kafka::increase_partitions(&env.brokers, &args.topic, args.total)?;
                }
                cli::AdminCommand::BatchCreateTopics(args) => {
                    kafka::batch_create_topics(
                        &env.brokers,
                        &args.topics,
                        args.partitions,
                        args.replication,
                    )?;
                }
                cli::AdminCommand::BatchDeleteTopics(args) => {
                    kafka::batch_delete_topics(&env.brokers, &args.topics)?;
                }
            }
        }
        cli::Command::Metrics(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            let metrics = kafka::get_cluster_metrics(&env.brokers)?;

            match args.format {
                cli::OutputFormat::Table => kafka::print_metrics_table(&metrics),
                cli::OutputFormat::Json => kafka::print_metrics_json(&metrics)?,
                cli::OutputFormat::Prometheus => kafka::print_metrics_prometheus(&metrics),
            }
        }
        cli::Command::Producer(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;
            kafka::produce_message(
                &env.brokers,
                &args.topic,
                &args.message,
                args.key.as_deref(),
                args.partition,
                &args.headers,
            )?;
        }
        cli::Command::Schema(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;

            let schema_config = env.schema_registry.as_ref().ok_or(
                "Schema registry not configured. Add schema_registry config to your environment.",
            )?;

            match args.command {
                cli::SchemaCommand::List => {
                    let mut client = schema::SchemaRegistryClient::new(schema_config)?;
                    let subjects = client.list_subjects()?;
                    if subjects.is_empty() {
                        println!("No subjects found in schema registry");
                    } else {
                        println!("Subjects:");
                        for subject in subjects {
                            println!("  {}", subject);
                        }
                    }
                }
                cli::SchemaCommand::Get(get_args) => {
                    let mut client = schema::SchemaRegistryClient::new(schema_config)?;
                    let schema_info =
                        client.get_schema_by_subject(&get_args.subject, &get_args.version)?;
                    println!("Subject: {}", schema_info.subject);
                    println!("Version: {}", schema_info.version);
                    println!("Schema ID: {}", schema_info.id);
                    println!("\nSchema:");
                    println!("{}", schema_info.schema);
                }
            }
        }
        cli::Command::Acl(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;

            match args.command {
                cli::AclCommand::List(list_args) => {
                    kafka::list_acls(
                        &env.brokers,
                        list_args.resource_type.as_deref(),
                        list_args.name.as_deref(),
                        list_args.principal.as_deref(),
                    )?;
                }
                cli::AclCommand::Create(create_args) => {
                    kafka::create_acl(
                        &env.brokers,
                        &create_args.resource_type,
                        &create_args.resource_name,
                        &create_args.resource_pattern,
                        &create_args.principal,
                        &create_args.operation,
                        &create_args.permission,
                        &create_args.host,
                    )?;
                }
                cli::AclCommand::Delete(delete_args) => {
                    kafka::delete_acl(
                        &env.brokers,
                        &delete_args.resource_type,
                        &delete_args.resource_name,
                        &delete_args.resource_pattern,
                        &delete_args.principal,
                        &delete_args.operation,
                        &delete_args.permission,
                        &delete_args.host,
                    )?;
                }
            }
        }
        cli::Command::Rebalance(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;

            match args.command {
                cli::RebalanceCommand::Status(status_args) => {
                    let statuses =
                        kafka::get_rebalance_status(&env.brokers, status_args.group.as_deref())?;
                    kafka::print_rebalance_status(&statuses, status_args.detailed);
                }
                cli::RebalanceCommand::Watch(watch_args) => {
                    kafka::watch_rebalancing(
                        &env.brokers,
                        watch_args.group.as_deref(),
                        watch_args.interval,
                    )?;
                }
            }
        }
        cli::Command::Offset(args) => {
            let config_file = get_config_file()?;
            let env = get_active_environment(config_file)?;

            match args.command {
                cli::OffsetCommand::Get(get_args) => {
                    let group_info = kafka::get_group_offsets(
                        &env.brokers,
                        &get_args.group,
                        get_args.topic.as_deref(),
                    )?;
                    kafka::print_group_offsets(&group_info, get_args.detailed);
                }
                cli::OffsetCommand::List(list_args) => {
                    let all_offsets =
                        kafka::list_all_group_offsets(&env.brokers, list_args.topic.as_deref())?;

                    if all_offsets.is_empty() {
                        println!("No consumer groups with offsets found.");
                    } else {
                        for group_info in &all_offsets {
                            kafka::print_group_offsets(group_info, false);
                        }
                    }
                }
                cli::OffsetCommand::Reset(reset_args) => {
                    // Confirmation prompt if not in dry-run and not using --yes
                    if !reset_args.dry_run && !reset_args.yes {
                        println!(
                            "\n⚠️  WARNING: You are about to reset offsets for consumer group '{}'",
                            reset_args.group
                        );
                        println!("   Topic: {}", reset_args.topic);
                        if let Some(p) = reset_args.partition {
                            println!("   Partition: {}", p);
                        } else {
                            println!("   All partitions will be affected");
                        }
                        println!("   Strategy: {}", reset_args.strategy);
                        println!("\n   This operation cannot be undone!");
                        print!("\nAre you sure you want to continue? (yes/no): ");
                        use std::io::{self, Write};
                        io::stdout().flush()?;

                        let mut input = String::new();
                        io::stdin().read_line(&mut input)?;
                        let input = input.trim().to_lowercase();

                        if input != "yes" && input != "y" {
                            println!("Operation cancelled.");
                            return Ok(());
                        }
                    }

                    let strategy = kafka::ResetStrategy::from_string(
                        &reset_args.strategy,
                        reset_args.timestamp,
                    )?;

                    let results = kafka::reset_group_offsets(
                        &env.brokers,
                        &reset_args.group,
                        &reset_args.topic,
                        reset_args.partition,
                        strategy,
                        reset_args.dry_run,
                    )?;

                    kafka::print_reset_results(&results, reset_args.dry_run);
                }
                cli::OffsetCommand::Seek(seek_args) => {
                    // Confirmation prompt if not in dry-run and not using --yes
                    if !seek_args.dry_run && !seek_args.yes {
                        println!(
                            "\n⚠️  WARNING: You are about to seek offset for consumer group '{}'",
                            seek_args.group
                        );
                        println!("   Topic: {}", seek_args.topic);
                        println!("   Partition: {}", seek_args.partition);
                        println!("   Target Offset: {}", seek_args.offset);
                        println!("\n   This operation cannot be undone!");
                        print!("\nAre you sure you want to continue? (yes/no): ");
                        use std::io::{self, Write};
                        io::stdout().flush()?;

                        let mut input = String::new();
                        io::stdin().read_line(&mut input)?;
                        let input = input.trim().to_lowercase();

                        if input != "yes" && input != "y" {
                            println!("Operation cancelled.");
                            return Ok(());
                        }
                    }

                    let result = kafka::seek_group_offset(
                        &env.brokers,
                        &seek_args.group,
                        &seek_args.topic,
                        seek_args.partition,
                        seek_args.offset,
                        seek_args.dry_run,
                    )?;

                    kafka::print_seek_result(&result, seek_args.dry_run);
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

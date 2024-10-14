use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

const CONFIG_FOLDER: &str = ".config/kcli";
const CONFIG_FILE: &str = "config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EnvironmentConfig {
    pub brokers: String,
    pub is_default: bool,
}

pub fn configure() {
    println!("Configuring kcli");
    let mut isOk = false;
    let mut environment = String::new();
    let mut brokers = String::new();
    while !isOk {
        environment = get_environment();
        brokers = get_kafka_brokers();

        println!("Are these values correct? (y/n)");
        println!("Environment: {}", environment);
        println!("Brokers: {}", brokers);

        io::stdout().flush().unwrap(); // Ensure the prompt is displayed before reading input
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        match input.trim() {
            "y" => isOk = true,
            "n" => continue,
            _ => {
                println!("Invalid input. Please enter 'y' or 'n'");
                continue;
            }
        }
    }

    // Create the config struct
    let config = EnvironmentConfig {
        brokers,
        is_default: false,
    };

    // Get config folder path
    let home_dir = env::var("HOME").expect("Could not get home directory");
    let config_folder = Path::new(&home_dir).join(CONFIG_FOLDER);

    if !config_folder.exists() {
        std::fs::create_dir_all(&config_folder).expect("Failed to create config folder");
        let config_path = Path::new(&home_dir).join(CONFIG_FOLDER).join(CONFIG_FILE);
        let _ = File::create(&config_path).expect("Failed to create config file");
    }

    // Read the existing config and remove the environment if it already exists
    let mut environments = read_config();
    if environments.contains_key(&environment) {
        environments.remove(&environment);
    }
    environments.insert(environment, config);
    let toml_string = toml::to_string(&environments).expect("Failed to serialize config");

    // Write the config to a file
    let config_path = config_folder.join(CONFIG_FILE);
    let mut file = File::create(&config_path).expect("Failed to create config file");
    file.write_all(toml_string.as_bytes())
        .expect("Failed to write to config file");

    println!("Configuration saved to {:?}", config_path);
}

fn get_environment() -> String {
    println!("Enter environment name");

    io::stdout().flush().unwrap(); // Ensure the prompt is displayed before reading input
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}

fn get_kafka_brokers() -> String {
    println!("Enter Kafka brokers");

    io::stdout().flush().unwrap(); // Ensure the prompt is displayed before reading input
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}

fn read_config() -> HashMap<String, EnvironmentConfig> {
    // Get the home directory
    let home_dir = env::var("HOME").expect("Could not get home directory");
    let config_path = Path::new(&home_dir).join(CONFIG_FOLDER).join(CONFIG_FILE);

    // Read the TOML file into a string
    let mut file = File::open(&config_path).expect("Failed to open config file");
    let mut toml_string = String::new();
    file.read_to_string(&mut toml_string)
        .expect("Failed to read config file");

    // Deserialize the string into a HashMap
    let environments: HashMap<String, EnvironmentConfig> =
        toml::from_str(&toml_string).expect("Failed to parse config file");

    environments
}

pub fn activate_environment(environment: &str) {
    let mut environments = read_config();
    if environments.contains_key(environment) {
        environments.iter_mut().for_each(|(key, value)| {
            value.is_default = key == environment;
        });
    } else {
        println!("Environment {} not found", environment);
        return;
    }

    let home_dir = env::var("HOME").expect("Could not get home directory");
    let config_folder = Path::new(&home_dir).join(CONFIG_FOLDER);
    let toml_string = toml::to_string(&environments).expect("Failed to serialize config");

    let config_path = config_folder.join(CONFIG_FILE);
    let mut file = File::create(&config_path).expect("Failed to create config file");
    file.write_all(toml_string.as_bytes())
        .expect("Failed to write to config file");

    println!("Environment {} activated", environment);
}

pub fn get_active_environment() -> Option<EnvironmentConfig> {
    let environments = read_config();
    let active_env = environments
        .iter()
        .find(|(_, config)| config.is_default)
        .map(|(_, env)| env.clone());

    active_env
}

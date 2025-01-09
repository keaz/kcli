use std::{
    collections::HashMap,
    env,
    fmt::format,
    fs::File,
    io::{self, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const CONFIG_FOLDER: &str = ".config/kcfli";
const CONFIG_FILE: &str = "config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EnvironmentConfig {
    pub brokers: String,
    pub is_default: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0}")]
    ConfigFileNotFound(String, #[source] std::io::Error),

    #[error("{0}")]
    HomeDirNotFound(String),

    #[error("{0}")]
    ConfigRead(String, #[source] std::io::Error),

    #[error("{0}")]
    ConfigWrite(String, #[source] std::io::Error),

    #[error("{0}")]
    ConfigParse(String, #[source] toml::de::Error),

    #[error("{0}")]
    ConfigSerialize(String, #[source] toml::ser::Error),

    #[error("{0}")]
    ConfigCreate(String, #[source] std::io::Error),

    #[error("{0}")]
    EnvironmentNotFound(String),

    #[error("{0}")]
    NoActiveEnvironment(String),
}

pub fn configure() -> Result<(), ConfigError> {
    println!("Configuring kcli");
    let mut is_ok = false;
    let mut environment = String::new();
    let mut brokers = String::new();
    while !is_ok {
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
            "y" => is_ok = true,
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
        std::fs::create_dir_all(&config_folder).map_err(|er| {
            ConfigError::ConfigCreate(format!("Failed to create {:?}", config_folder.to_str()), er)
        })?;
        let config_path = Path::new(&home_dir).join(CONFIG_FOLDER).join(CONFIG_FILE);
        let _ = File::create(&config_path).map_err(|er| {
            ConfigError::ConfigCreate(format!("Failed to create {:?}", config_path.to_str()), er)
        })?;
    }

    let file = get_config_file()?;
    // Read the existing config and remove the environment if it already exists
    let mut environments = read_config(file)?;
    if environments.contains_key(&environment) {
        environments.remove(&environment);
    }

    environments.insert(environment, config);
    let toml_string = toml::to_string(&environments).map_err(|err| {
        ConfigError::ConfigSerialize("Failed to serialize config".to_string(), err)
    })?;

    // Write the config to a file
    let config_path = config_folder.join(CONFIG_FILE);
    let mut file = File::create(&config_path).map_err(|er| {
        ConfigError::ConfigCreate(
            format!("Failed to create config file: {:?}", config_path),
            er,
        )
    })?;

    file.write_all(toml_string.as_bytes()).map_err(|er| {
        ConfigError::ConfigWrite(
            format!("Failed to write to config file: {:?}", config_path),
            er,
        )
    })?;

    println!("Configuration saved to {:?}", config_path);
    Ok(())
}

fn get_environment() -> String {
    println!("Enter environment name");
    read_user_inout()
}

fn get_kafka_brokers() -> String {
    println!("Enter Kafka brokers");
    read_user_inout()
}

fn read_user_inout() -> String {
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}

fn read_config(mut config_file: File) -> Result<HashMap<String, EnvironmentConfig>, ConfigError> {
    let mut toml_string = String::new();
    config_file.read_to_string(&mut toml_string).map_err(|er| {
        ConfigError::ConfigRead(format!("Failed to read config file: {:?}", config_file), er)
    })?;

    // Deserialize the string into a HashMap
    let environments: HashMap<String, EnvironmentConfig> = toml::from_str(&toml_string)
        .map_err(|er| ConfigError::ConfigParse("Failed to parse config".to_string(), er))?;

    Ok(environments)
}

pub fn activate_environment(environment: &str, config_file: File) -> Result<(), ConfigError> {
    let mut environments = read_config(config_file)?;

    if environments.contains_key(environment) {
        environments.iter_mut().for_each(|(key, value)| {
            value.is_default = key == environment;
        });
    } else {
        return Err(ConfigError::EnvironmentNotFound(format!(
            "Environment {} not found",
            environment
        )));
    }

    let toml_string = toml::to_string(&environments)
        .map_err(|er| ConfigError::ConfigSerialize("Failed to serialize config".to_string(), er))?;

    let mut file = get_config_file()?;

    file.write_all(toml_string.as_bytes()).map_err(|er| {
        ConfigError::ConfigWrite(format!("Failed to write to config file: {:?}", file), er)
    })?;

    println!("Environment {} activated", environment);
    Ok(())
}

pub fn get_config_file() -> Result<File, ConfigError> {
    // Get the home directory
    let home_dir = env::var("HOME").map_err(|er| {
        ConfigError::HomeDirNotFound("HOME environment variable not found".to_string())
    })?;
    let config_path = Path::new(&home_dir).join(CONFIG_FOLDER).join(CONFIG_FILE);

    // Read the TOML file into a string
    let file = File::open(&config_path).map_err(|er| {
        ConfigError::ConfigFileNotFound(
            format!("Failed to open config file: {:?}", config_path),
            er,
        )
    })?;

    Ok(file)
}

pub fn get_active_environment(config_file: File) -> Result<EnvironmentConfig, ConfigError> {
    let environments = read_config(config_file)?;
    let active_env = environments
        .iter()
        .find(|(_, config)| config.is_default)
        .map(|(_, env)| env.clone());

    if active_env.is_none() {
        return Err(ConfigError::NoActiveEnvironment(
            "No active environment found".to_string(),
        ));
    }
    Ok(active_env.unwrap())
}

#[cfg(test)]
mod test {

    #[test]
    fn test_read_config() {}

    #[test]
    fn test_get_active_environment() {}
}

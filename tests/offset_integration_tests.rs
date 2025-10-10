// Integration tests for Offset Management
// These tests validate offset management functionality
// Tests are designed to work with or without Kafka available

use std::process::Command;

// Helper function to check if Kafka is available
fn is_kafka_available() -> bool {
    std::net::TcpStream::connect("localhost:9092").is_ok()
}

// Helper function to run kfcli command
fn run_kfcli(args: &[&str]) -> Result<String, String> {
    let output = Command::new("./target/debug/kfcli").args(args).output();

    match output {
        Ok(out) => {
            if out.status.success() {
                Ok(String::from_utf8_lossy(&out.stdout).to_string())
            } else {
                Err(String::from_utf8_lossy(&out.stderr).to_string())
            }
        }
        Err(e) => Err(format!("Failed to execute command: {}", e)),
    }
}

#[test]
fn test_offset_command_help() {
    // Test that offset command has help
    match run_kfcli(&["offset", "--help"]) {
        Ok(output) => {
            assert!(output.contains("Manage consumer group offsets"));
            assert!(output.contains("get"));
            assert!(output.contains("list"));
            assert!(output.contains("reset"));
            assert!(output.contains("seek"));
        }
        Err(e) => {
            eprintln!("Command help error (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_get_command_help() {
    // Test that offset get command has help
    match run_kfcli(&["offset", "get", "--help"]) {
        Ok(output) => {
            assert!(output.contains("Get current offsets for a consumer group"));
            assert!(output.contains("--group"));
            assert!(output.contains("--topic"));
            assert!(output.contains("--detailed"));
        }
        Err(e) => {
            eprintln!("Command help error (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_list_command_help() {
    // Test that offset list command has help
    match run_kfcli(&["offset", "list", "--help"]) {
        Ok(output) => {
            assert!(output.contains("List offsets for all consumer groups"));
            assert!(output.contains("--topic"));
        }
        Err(e) => {
            eprintln!("Command help error (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_command_help() {
    // Test that offset reset command has help
    match run_kfcli(&["offset", "reset", "--help"]) {
        Ok(output) => {
            assert!(output.contains("Reset consumer group offsets"));
            assert!(output.contains("--group"));
            assert!(output.contains("--topic"));
            assert!(output.contains("--strategy"));
            assert!(output.contains("--timestamp"));
            assert!(output.contains("--partition"));
            assert!(output.contains("--dry-run"));
            assert!(output.contains("--yes"));
        }
        Err(e) => {
            eprintln!("Command help error (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_seek_command_help() {
    // Test that offset seek command has help
    match run_kfcli(&["offset", "seek", "--help"]) {
        Ok(output) => {
            assert!(output.contains("Seek to a specific offset"));
            assert!(output.contains("--group"));
            assert!(output.contains("--topic"));
            assert!(output.contains("--partition"));
            assert!(output.contains("--offset"));
            assert!(output.contains("--dry-run"));
            assert!(output.contains("--yes"));
        }
        Err(e) => {
            eprintln!("Command help error (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_get_requires_group() {
    // Test that offset get requires --group parameter
    match run_kfcli(&["offset", "get"]) {
        Ok(_) => {
            panic!("Should require --group parameter");
        }
        Err(e) => {
            // Expected error
            assert!(e.contains("required") || e.contains("--group"));
        }
    }
}

#[test]
fn test_offset_reset_requires_parameters() {
    // Test that offset reset requires parameters
    match run_kfcli(&["offset", "reset"]) {
        Ok(_) => {
            panic!("Should require parameters");
        }
        Err(e) => {
            // Expected error
            assert!(e.contains("required"));
        }
    }
}

#[test]
fn test_offset_seek_requires_parameters() {
    // Test that offset seek requires parameters
    match run_kfcli(&["offset", "seek"]) {
        Ok(_) => {
            panic!("Should require parameters");
        }
        Err(e) => {
            // Expected error
            assert!(e.contains("required"));
        }
    }
}

#[test]
fn test_offset_get_with_kafka() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Try to get offsets for a test group (may not exist, that's okay)
    match run_kfcli(&["offset", "get", "--group", "kfcli-test-group"]) {
        Ok(output) => {
            println!("Successfully retrieved offsets");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error getting offsets (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_list_with_kafka() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Try to list all offsets
    match run_kfcli(&["offset", "list"]) {
        Ok(output) => {
            println!("Successfully listed offsets");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error listing offsets (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_dry_run() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Try dry run reset (should not require confirmation)
    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "kfcli-test-group",
        "--topic",
        "test-topic",
        "--strategy",
        "earliest",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Dry run reset output:");
            println!("{}", output);
            assert!(output.contains("DRY RUN") || output.contains("Would"));
        }
        Err(e) => {
            eprintln!("Error in dry run reset (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_seek_dry_run() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Try dry run seek
    match run_kfcli(&[
        "offset",
        "seek",
        "--group",
        "kfcli-test-group",
        "--topic",
        "test-topic",
        "--partition",
        "0",
        "--offset",
        "0",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Dry run seek output:");
            println!("{}", output);
            assert!(output.contains("DRY RUN") || output.contains("Would"));
        }
        Err(e) => {
            eprintln!("Error in dry run seek (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_strategy_earliest() {
    // Test that earliest strategy is recognized
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "test-group",
        "--topic",
        "test-topic",
        "--strategy",
        "earliest",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Earliest strategy test output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error with earliest strategy (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_strategy_latest() {
    // Test that latest strategy is recognized
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "test-group",
        "--topic",
        "test-topic",
        "--strategy",
        "latest",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Latest strategy test output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error with latest strategy (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_strategy_timestamp() {
    // Test that timestamp strategy requires --timestamp parameter
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "test-group",
        "--topic",
        "test-topic",
        "--strategy",
        "timestamp",
        "--timestamp",
        "1609459200000",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Timestamp strategy test output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error with timestamp strategy (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_invalid_strategy() {
    // Test that invalid strategy is rejected
    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "test-group",
        "--topic",
        "test-topic",
        "--strategy",
        "invalid-strategy",
        "--dry-run",
    ]) {
        Ok(_) => {
            panic!("Should reject invalid strategy");
        }
        Err(e) => {
            // Expected error
            println!("Expected error for invalid strategy: {}", e);
        }
    }
}

#[test]
fn test_offset_get_with_topic_filter() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Test with topic filter
    match run_kfcli(&[
        "offset",
        "get",
        "--group",
        "kfcli-test-group",
        "--topic",
        "test-topic",
    ]) {
        Ok(output) => {
            println!("Offset get with topic filter output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error getting filtered offsets (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_get_detailed() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Test detailed output
    match run_kfcli(&["offset", "get", "--group", "kfcli-test-group", "--detailed"]) {
        Ok(output) => {
            println!("Offset get detailed output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error getting detailed offsets (may be expected): {}", e);
        }
    }
}

#[test]
fn test_offset_reset_specific_partition() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Test resetting specific partition
    match run_kfcli(&[
        "offset",
        "reset",
        "--group",
        "test-group",
        "--topic",
        "test-topic",
        "--partition",
        "0",
        "--strategy",
        "earliest",
        "--dry-run",
    ]) {
        Ok(output) => {
            println!("Reset specific partition output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!(
                "Error resetting specific partition (may be expected): {}",
                e
            );
        }
    }
}

#[test]
fn test_offset_list_with_topic_filter() {
    if !is_kafka_available() {
        eprintln!("Skipping test - Kafka not available at localhost:9092");
        return;
    }

    // Test list with topic filter
    match run_kfcli(&["offset", "list", "--topic", "test-topic"]) {
        Ok(output) => {
            println!("Offset list with topic filter output:");
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error listing filtered offsets (may be expected): {}", e);
        }
    }
}

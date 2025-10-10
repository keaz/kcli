# Offset Management Guide

## Table of Contents
- [Overview](#overview)
- [What are Consumer Group Offsets?](#what-are-consumer-group-offsets)
- [Commands](#commands)
  - [Get Offsets](#get-offsets)
  - [List Offsets](#list-offsets)
  - [Reset Offsets](#reset-offsets)
  - [Seek Offset](#seek-offset)
- [Understanding Offset Strategies](#understanding-offset-strategies)
- [Common Use Cases](#common-use-cases)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Implementation Details](#implementation-details)

## Overview

The offset management feature in `kfcli` provides comprehensive tools to view, manage, and manipulate consumer group offsets in Apache Kafka. Offsets are critical for tracking which messages have been consumed by consumer groups.

### Key Features
- **View Offsets**: Get current offset positions and lag for consumer groups
- **List All Offsets**: See offsets for all consumer groups
- **Reset Offsets**: Reset offsets to earliest, latest, or specific timestamp
- **Seek Offsets**: Set offsets to specific positions
- **Dry-Run Mode**: Preview changes before executing
- **Safety Features**: Confirmation prompts for destructive operations

## What are Consumer Group Offsets?

In Kafka, **offsets** are positions in a topic partition that mark which messages have been consumed by a consumer group. Each consumer group maintains its own set of offsets for the topics it subscribes to.

### Key Concepts

**Current Offset**: The position of the last message consumed by the consumer group  
**Log End Offset**: The position of the last message in the partition (latest available)  
**Lag**: The difference between the log end offset and current offset (messages behind)

```
Topic Partition: [msg0][msg1][msg2][msg3][msg4][msg5][msg6][msg7][msg8]
                            ^                                   ^
                     Current Offset (2)               Log End Offset (8)
                            |_____________Lag = 6_______________|
```

## Commands

### Get Offsets

Retrieve current offset information for a specific consumer group.

#### Syntax
```bash
kfcli offset get --group <GROUP> [OPTIONS]
```

#### Options
- `--group <GROUP>` (required): Consumer group name
- `--topic <TOPIC>` (optional): Filter by specific topic
- `--detailed` (optional): Show partition-level details

#### Examples

**Get offsets for a consumer group:**
```bash
kfcli offset get --group my-consumer-group
```

**Get offsets with partition details:**
```bash
kfcli offset get --group my-consumer-group --detailed
```

**Get offsets for specific topic:**
```bash
kfcli offset get --group my-consumer-group --topic orders --detailed
```

#### Sample Output

**Summary View:**
```
╔══════════════════════════════════════════════════════════════╗
║ Consumer Group: my-consumer-group                            ║
║ State: Stable                                                ║
║ Total Lag: 1250                                              ║
╚══════════════════════════════════════════════════════════════╝

+---------+------------+-----------+----------------+
| Topic   | Partitions | Total Lag | Total Messages |
+---------+------------+-----------+----------------+
| orders  | 3          | 750       | 15000          |
| events  | 2          | 500       | 10000          |
+---------+------------+-----------+----------------+
```

**Detailed View:**
```
+---------+-----------+----------------+---------+-----+
| Topic   | Partition | Current Offset | Log End | Lag |
+---------+-----------+----------------+---------+-----+
| orders  | 0         | 4800           | 5000    | 200 |
| orders  | 1         | 4750           | 5000    | 250 |
| orders  | 2         | 4700           | 5000    | 300 |
| events  | 0         | 4750           | 5000    | 250 |
| events  | 1         | 4750           | 5000    | 250 |
+---------+-----------+----------------+---------+-----+
```

### List Offsets

List offsets for all consumer groups in the cluster.

#### Syntax
```bash
kfcli offset list [OPTIONS]
```

#### Options
- `--topic <TOPIC>` (optional): Filter by specific topic

#### Examples

**List all consumer group offsets:**
```bash
kfcli offset list
```

**List offsets for groups consuming a specific topic:**
```bash
kfcli offset list --topic orders
```

#### Sample Output
```
╔══════════════════════════════════════════════════════════════╗
║ Consumer Group: service-a-group                              ║
║ State: Stable                                                ║
║ Total Lag: 0                                                 ║
╚══════════════════════════════════════════════════════════════╝

+---------+------------+-----------+----------------+
| Topic   | Partitions | Total Lag | Total Messages |
+---------+------------+-----------+----------------+
| orders  | 3          | 0         | 15000          |
+---------+------------+-----------+----------------+

╔══════════════════════════════════════════════════════════════╗
║ Consumer Group: service-b-group                              ║
║ State: Stable                                                ║
║ Total Lag: 500                                               ║
╚══════════════════════════════════════════════════════════════╝

+---------+------------+-----------+----------------+
| Topic   | Partitions | Total Lag | Total Messages |
+---------+------------+-----------+----------------+
| events  | 2          | 500       | 10000          |
+---------+------------+-----------+----------------+
```

### Reset Offsets

Reset consumer group offsets to a specific position.

⚠️ **WARNING**: This is a destructive operation that cannot be undone!

#### Syntax
```bash
kfcli offset reset --group <GROUP> --topic <TOPIC> --strategy <STRATEGY> [OPTIONS]
```

#### Options
- `--group <GROUP>` (required): Consumer group name
- `--topic <TOPIC>` (required): Topic name
- `--strategy <STRATEGY>` (required): Reset strategy (earliest, latest, timestamp)
- `--timestamp <TIMESTAMP>` (required for timestamp strategy): Unix timestamp in milliseconds
- `--partition <PARTITION>` (optional): Specific partition (resets all if not specified)
- `--dry-run` (optional): Preview changes without executing
- `--yes` (optional): Skip confirmation prompt

#### Reset Strategies

1. **`earliest`**: Reset to the beginning of the topic (oldest available message)
2. **`latest`**: Reset to the end of the topic (next message to be produced)
3. **`timestamp`**: Reset to messages produced after a specific timestamp

#### Examples

**Dry-run reset to earliest:**
```bash
kfcli offset reset --group my-group --topic orders --strategy earliest --dry-run
```

**Reset all partitions to earliest:**
```bash
kfcli offset reset --group my-group --topic orders --strategy earliest
```

**Reset specific partition to latest:**
```bash
kfcli offset reset --group my-group --topic orders --partition 0 --strategy latest
```

**Reset to timestamp (with prompt):**
```bash
# Reset to January 1, 2024 00:00:00 UTC (1704067200000 ms)
kfcli offset reset --group my-group --topic orders --strategy timestamp --timestamp 1704067200000
```

**Reset to timestamp (skip confirmation):**
```bash
kfcli offset reset --group my-group --topic orders --strategy timestamp --timestamp 1704067200000 --yes
```

#### Sample Output

**Dry-Run Mode:**
```
=== DRY RUN - No changes will be made ===

+--------+-----------+------------+------------+---------------+
| Topic  | Partition | Old Offset | New Offset | Status        |
+--------+-----------+------------+------------+---------------+
| orders | 0         | 5000       | 0          | Would reset ✓ |
| orders | 1         | 5000       | 0          | Would reset ✓ |
| orders | 2         | 5000       | 0          | Would reset ✓ |
+--------+-----------+------------+------------+---------------+
```

**Actual Reset:**
```
⚠️  WARNING: You are about to reset offsets for consumer group 'my-group'
   Topic: orders
   All partitions will be affected
   Strategy: earliest

   This operation cannot be undone!

Are you sure you want to continue? (yes/no): yes

=== Offset Reset Results ===

+--------+-----------+------------+------------+-----------+
| Topic  | Partition | Old Offset | New Offset | Status    |
+--------+-----------+------------+------------+-----------+
| orders | 0         | 5000       | 0          | Success ✓ |
| orders | 1         | 5000       | 0          | Success ✓ |
| orders | 2         | 5000       | 0          | Success ✓ |
+--------+-----------+------------+------------+-----------+
```

### Seek Offset

Set an offset to a specific position for a particular partition.

⚠️ **WARNING**: This is a destructive operation that cannot be undone!

#### Syntax
```bash
kfcli offset seek --group <GROUP> --topic <TOPIC> --partition <PARTITION> --offset <OFFSET> [OPTIONS]
```

#### Options
- `--group <GROUP>` (required): Consumer group name
- `--topic <TOPIC>` (required): Topic name
- `--partition <PARTITION>` (required): Partition number
- `--offset <OFFSET>` (required): Target offset position
- `--dry-run` (optional): Preview changes without executing
- `--yes` (optional): Skip confirmation prompt

#### Examples

**Dry-run seek to offset 1000:**
```bash
kfcli offset seek --group my-group --topic orders --partition 0 --offset 1000 --dry-run
```

**Seek to offset 1000:**
```bash
kfcli offset seek --group my-group --topic orders --partition 0 --offset 1000
```

**Seek without confirmation:**
```bash
kfcli offset seek --group my-group --topic orders --partition 0 --offset 1000 --yes
```

#### Sample Output

**Dry-Run Mode:**
```
=== DRY RUN - No changes will be made ===

+----------+--------+-----------+------------+------------+-------------+
| Group    | Topic  | Partition | Old Offset | New Offset | Status      |
+----------+--------+-----------+------------+------------+-------------+
| my-group | orders | 0         | 5000       | 1000       | Would seek ✓|
+----------+--------+-----------+------------+------------+-------------+
```

**Actual Seek:**
```
⚠️  WARNING: You are about to seek offset for consumer group 'my-group'
   Topic: orders
   Partition: 0
   Target Offset: 1000

   This operation cannot be undone!

Are you sure you want to continue? (yes/no): yes

=== Offset Seek Result ===

+----------+--------+-----------+------------+------------+-----------+
| Group    | Topic  | Partition | Old Offset | New Offset | Status    |
+----------+--------+-----------+------------+------------+-----------+
| my-group | orders | 0         | 5000       | 1000       | Success ✓ |
+----------+--------+-----------+------------+------------+-----------+
```

## Understanding Offset Strategies

### Earliest Strategy

Resets offsets to the **beginning of the topic** (oldest available message).

**Use Cases:**
- Reprocess all available messages from the start
- Recover from data processing errors
- Backfill data

**Example:**
```bash
kfcli offset reset --group my-group --topic orders --strategy earliest
```

**Effect:**
- Offset set to 0 (or earliest available offset if log retention has removed older messages)
- Consumer will reprocess all messages from the beginning

### Latest Strategy

Resets offsets to the **end of the topic** (next message to be produced).

**Use Cases:**
- Skip all existing messages and start fresh
- Ignore backlog and only process new messages
- Start consuming from "now"

**Example:**
```bash
kfcli offset reset --group my-group --topic orders --strategy latest
```

**Effect:**
- Offset set to log end offset
- Consumer will only process new messages produced after the reset

### Timestamp Strategy

Resets offsets to the first message produced **after a specific timestamp**.

**Use Cases:**
- Reprocess messages from a specific point in time
- Recover from issues that started at a known time
- Time-based data replay

**Example:**
```bash
# Reset to messages after January 1, 2024 00:00:00 UTC
kfcli offset reset --group my-group --topic orders \
  --strategy timestamp --timestamp 1704067200000
```

**Effect:**
- Offset set to first message with timestamp >= specified value
- Consumer will process messages from that point forward

**Getting Timestamps:**
```bash
# Current time in milliseconds
date +%s000

# Specific date (on macOS)
date -j -f "%Y-%m-%d %H:%M:%S" "2024-01-01 00:00:00" +%s000

# Specific date (on Linux)
date -d "2024-01-01 00:00:00" +%s000
```

## Common Use Cases

### 1. Reprocessing All Messages

**Scenario**: Need to reprocess all messages due to a bug fix or new business logic.

```bash
# First, check current offsets
kfcli offset get --group my-processor-group --detailed

# Preview the reset
kfcli offset reset --group my-processor-group --topic events \
  --strategy earliest --dry-run

# Execute the reset
kfcli offset reset --group my-processor-group --topic events \
  --strategy earliest
```

### 2. Skipping Backlog

**Scenario**: Consumer is far behind and you want to skip the backlog to process only new messages.

```bash
# Check current lag
kfcli offset get --group my-slow-consumer --detailed

# Reset to latest
kfcli offset reset --group my-slow-consumer --topic orders \
  --strategy latest --yes
```

### 3. Recovering from a Known Issue

**Scenario**: Issue started at a specific time, need to reprocess from that point.

```bash
# Determine the timestamp when the issue started
# Example: Issue started at 2024-01-15 10:30:00 UTC

# Convert to milliseconds: 1705318200000

# Reset to that timestamp
kfcli offset reset --group my-processor-group --topic orders \
  --strategy timestamp --timestamp 1705318200000
```

### 4. Debugging Specific Messages

**Scenario**: Need to replay specific messages for debugging.

```bash
# Get offset range
kfcli offset get --group my-group --topic debug-topic --detailed

# Seek to specific offset
kfcli offset seek --group my-group --topic debug-topic \
  --partition 0 --offset 12345
```

### 5. Balancing Partition Consumption

**Scenario**: One partition is lagging behind others.

```bash
# Check partition lag
kfcli offset get --group my-group --topic orders --detailed

# Reset only the lagging partition
kfcli offset reset --group my-group --topic orders \
  --partition 2 --strategy latest
```

## Best Practices

### 1. Always Use Dry-Run First

Before executing any destructive operation, use `--dry-run` to preview changes:

```bash
kfcli offset reset --group my-group --topic orders \
  --strategy earliest --dry-run
```

### 2. Stop Consumers Before Resetting

To avoid conflicts:

1. Stop all consumers in the target group
2. Reset offsets
3. Restart consumers

```bash
# 1. Stop consumers (using your deployment tool)
kubectl scale deployment my-consumer --replicas=0

# 2. Reset offsets
kfcli offset reset --group my-group --topic orders --strategy earliest

# 3. Restart consumers
kubectl scale deployment my-consumer --replicas=3
```

### 3. Monitor After Reset

After resetting offsets, monitor consumer lag:

```bash
# Monitor in real-time
watch -n 5 'kfcli offset get --group my-group --detailed'

# Or use rebalance monitoring
kfcli rebalance watch --group my-group
```

### 4. Document Offset Changes

Keep a log of offset resets for troubleshooting:

```bash
# Log the reset
echo "$(date): Reset my-group on orders to earliest" >> offset-changes.log

# Include current offsets before reset
kfcli offset get --group my-group --topic orders --detailed >> offset-changes.log
```

### 5. Use Specific Partitions When Possible

Reset individual partitions rather than all:

```bash
# Better (specific partition)
kfcli offset reset --group my-group --topic orders \
  --partition 0 --strategy latest

# Avoid (all partitions)
kfcli offset reset --group my-group --topic orders --strategy latest
```

### 6. Be Careful with Production Data

- Always test offset operations in development/staging first
- Use `--dry-run` in production
- Have a rollback plan
- Consider data duplication impacts

## Troubleshooting

### Consumer Group Not Found

**Problem**: Error message "Consumer group 'my-group' not found"

**Solution**:
```bash
# List all consumer groups
kfcli consumer list

# Verify the group name is correct
kfcli offset list
```

### Topic Not Found

**Problem**: Error message "Topic 'my-topic' does not exist"

**Solution**:
```bash
# List all topics
kfcli topics list

# Verify the topic name is correct
```

### Partition Out of Bounds

**Problem**: Error message "Partition 5 does not exist for topic 'orders'"

**Solution**:
```bash
# Check available partitions
kfcli topics details --topic orders

# Use valid partition number
kfcli offset seek --group my-group --topic orders --partition 2 --offset 1000
```

### Offset Out of Range

**Problem**: Error message "Target offset 99999 is out of bounds [0..1000]"

**Solution**:
```bash
# Check valid offset range
kfcli offset get --group my-group --topic orders --detailed

# Use an offset within the range
kfcli offset seek --group my-group --topic orders --partition 0 --offset 500
```

### Permission Denied

**Problem**: Error message "ACL operation failed"

**Solution**:
- Ensure you have proper ACL permissions
- Check Kafka broker configuration
- Contact your Kafka administrator

```bash
# Check ACL permissions
kfcli acl list --resource-type group --name my-group
```

### Consumer Group in Rebalancing State

**Problem**: Offsets change unexpectedly or reset fails

**Solution**:
```bash
# Check consumer group state
kfcli rebalance status --group my-group

# Wait for stable state or stop consumers
```

### Timestamp Strategy Not Working

**Problem**: Timestamp reset doesn't work as expected

**Note**: The current implementation uses watermarks as a fallback for timestamp strategy. For precise timestamp-based resets, you may need to use native Kafka tools:

```bash
# Alternative using kafka-consumer-groups.sh
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --topic orders --reset-offsets \
  --to-datetime 2024-01-01T00:00:00.000 --execute
```

## Implementation Details

### Offset Storage

Kafka stores consumer group offsets in a special internal topic called `__consumer_offsets`. When you commit or reset offsets, they are written to this topic.

### Offset Commit

Offsets are typically committed:
- Automatically by consumers (auto-commit enabled)
- Manually by consumers after processing messages
- Via `kfcli` offset commands

### Safety Features

1. **Dry-Run Mode**: Preview changes without executing
2. **Confirmation Prompts**: Interactive confirmation for destructive operations
3. **Validation**: Checks for valid topics, partitions, and offset ranges
4. **Error Handling**: Graceful error messages for common issues

### Performance Considerations

- Offset operations are lightweight metadata operations
- No impact on message consumption or production
- Safe to run on production clusters
- Commands typically complete in <1 second

### Limitations

1. **Timestamp Strategy**: Uses watermarks as fallback (implementation note)
2. **Active Consumers**: May cause conflicts if consumers are running
3. **Committed Offsets Only**: Can only manage offsets that have been committed
4. **No History**: Cannot view historical offset changes (use external monitoring)

### Related Commands

- `kfcli rebalance status`: Check consumer group state before resetting offsets
- `kfcli rebalance watch`: Monitor consumer group behavior after offset changes
- `kfcli consumer list`: List all consumer groups
- `kfcli topics details`: Get topic partition information

## Examples Summary

```bash
# View offsets
kfcli offset get --group my-group
kfcli offset get --group my-group --detailed
kfcli offset list

# Reset offsets (with dry-run first)
kfcli offset reset --group my-group --topic orders --strategy earliest --dry-run
kfcli offset reset --group my-group --topic orders --strategy earliest

# Reset to latest (skip backlog)
kfcli offset reset --group my-group --topic orders --strategy latest --yes

# Reset to timestamp
kfcli offset reset --group my-group --topic orders \
  --strategy timestamp --timestamp 1704067200000

# Reset specific partition
kfcli offset reset --group my-group --topic orders \
  --partition 0 --strategy latest

# Seek to specific offset
kfcli offset seek --group my-group --topic orders \
  --partition 0 --offset 1000 --dry-run

kfcli offset seek --group my-group --topic orders \
  --partition 0 --offset 1000 --yes
```

## Conclusion

Offset management is a powerful feature that requires careful use. Always:
- Use `--dry-run` first
- Stop consumers before resetting
- Monitor after changes
- Document your actions
- Have a rollback plan

For questions or issues, refer to the main kfcli documentation or your Kafka administrator.

---

**Version**: kfcli 0.2.1-alpha  
**Last Updated**: October 10, 2025

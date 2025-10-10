# Rebalance Monitoring Guide

## Overview

The rebalance monitoring feature in `kfcli` allows you to track consumer group rebalancing events in your Kafka cluster. Rebalancing occurs when consumers join or leave a group, or when partition assignments change, and monitoring these events is crucial for understanding consumer group behavior and debugging issues.

## What is Rebalancing?

Rebalancing is the process by which Kafka redistributes partitions among consumers in a consumer group. This happens when:
- A new consumer joins the group
- A consumer leaves the group (gracefully or due to failure)
- The number of partitions in subscribed topics changes
- A consumer is considered dead (hasn't sent heartbeat within session timeout)

During rebalancing, consumers temporarily stop consuming messages, which can cause processing delays.

## Commands

### Check Rebalance Status

View the current state of consumer groups and detect if rebalancing is in progress:

```bash
# Show status for all consumer groups
kfcli rebalance status

# Show status for a specific consumer group
kfcli rebalance status --group my-consumer-group

# Show detailed partition assignment information
kfcli rebalance status --detailed
kfcli rebalance status --group my-consumer-group --detailed
```

**Output Includes:**
- Consumer group name
- Current state (Stable, PreparingRebalance, CompletingRebalance, etc.)
- Rebalancing indicator (✓ Stable or ⚠️ REBALANCING)
- Total number of partitions assigned
- Number of active members
- Partition distribution across members (detailed mode)
- Per-topic partition assignments (detailed mode)

### Watch for Rebalancing Events

Monitor consumer groups in real-time and get notified when rebalancing occurs:

```bash
# Watch all consumer groups
kfcli rebalance watch

# Watch a specific consumer group
kfcli rebalance watch --group my-consumer-group

# Watch with custom polling interval (default: 5 seconds)
kfcli rebalance watch --interval 10

# Watch specific group with custom interval
kfcli rebalance watch --group my-consumer-group --interval 3
```

**Watch Mode Features:**
- Real-time state change notifications
- Partition redistribution alerts
- Timestamp for each event
- Shows which consumers gained or lost partitions
- Visual indicators (🔄 for state changes, 📊 for distribution changes)
- Runs continuously until stopped (Ctrl+C)

## Status Output Examples

### Stable Consumer Group

```
═══════════════════════════════════════════════════════════
Consumer Group: my-consumer-group
Status: ✓ Stable - Stable
Total Partitions: 8
Members: 2

Partition Distribution:
  consumer-1: 4 partitions
  consumer-2: 4 partitions
═══════════════════════════════════════════════════════════
```

### Rebalancing Consumer Group

```
═══════════════════════════════════════════════════════════
Consumer Group: my-consumer-group
Status: ⚠️  REBALANCING - PreparingRebalance
Total Partitions: 8
Members: 3
═══════════════════════════════════════════════════════════
```

### Detailed Output

```
═══════════════════════════════════════════════════════════
Consumer Group: my-consumer-group
Status: ✓ Stable - Stable
Total Partitions: 8
Members: 2

Member Details:
+----------------------+------------+------------------+---------------------+
| Member ID            | Client ID  | Host             | Assigned Partitions |
+----------------------+------------+------------------+---------------------+
| consumer-1-12345...  | consumer-1 | /192.168.1.100  | 4                   |
| consumer-2-67890...  | consumer-2 | /192.168.1.101  | 4                   |
+----------------------+------------+------------------+---------------------+

Partition Distribution:

  Topic: orders
+------------+------------+
| Client ID  | Partitions |
+------------+------------+
| consumer-1 | 0, 1, 2, 3 |
| consumer-2 | 4, 5, 6, 7 |
+------------+------------+
═══════════════════════════════════════════════════════════
```

## Watch Mode Output Examples

### State Change Detection

```
Watching for rebalancing events... (Press Ctrl+C to stop)

[2025-10-10 14:23:15] 🔄 Group 'my-consumer-group': State changed Stable -> PreparingRebalance
    ⚠️  Rebalancing in progress!

[2025-10-10 14:23:22] 🔄 Group 'my-consumer-group': State changed PreparingRebalance -> Stable
    ✓ Rebalancing completed
```

### Partition Redistribution

```
[2025-10-10 14:25:30] 📊 Group 'my-consumer-group': Partition distribution changed
    ↑ consumer-3: 3 partitions (was 0)
    ↓ consumer-1: 2 partitions (was 4)
    ↓ consumer-2: 3 partitions (was 4)
```

## Understanding Consumer Group States

### Stable States
- **Stable**: Normal operation, consumers are actively consuming messages
- **Active**: Consumers are connected and functioning normally

### Rebalancing States
- **PreparingRebalance**: Group coordinator is preparing for rebalance
- **CompletingRebalance**: Rebalance is finalizing, new assignments being distributed
- **Empty**: Group has no active members

### Transitional Indicators
- Members without partition assignments (total_partitions = 0 but members > 0)
- This indicates consumers are connected but haven't received assignments yet

## Common Use Cases

### 1. Debugging Consumer Group Issues

Check if a consumer group is stuck in rebalancing:
```bash
kfcli rebalance status --group problematic-group --detailed
```

### 2. Monitoring Consumer Scaling

Watch for partition redistribution when scaling consumers:
```bash
# In one terminal, watch the group
kfcli rebalance watch --group my-group

# In another terminal, start new consumer instances
# Watch mode will show partition redistribution
```

### 3. Detecting Consumer Failures

Monitor for unexpected rebalancing that might indicate consumer crashes:
```bash
kfcli rebalance watch --interval 3
```

If you see frequent rebalancing, it might indicate:
- Consumer instances crashing
- Network issues
- Session timeout too short
- Max poll interval exceeded

### 4. Verifying Partition Distribution

Check if partitions are evenly distributed:
```bash
kfcli rebalance status --group my-group --detailed
```

Look for:
- Uneven partition distribution (some consumers with many more partitions)
- Consumers without assignments
- Expected number of active members

### 5. Planning Consumer Group Changes

Before making changes (scaling up/down, configuration updates):
```bash
# 1. Check current status
kfcli rebalance status --group my-group --detailed

# 2. Start watching
kfcli rebalance watch --group my-group

# 3. Make your changes
# 4. Observe rebalancing behavior and verify stable state
```

## Implementation Details

### Rebalancing Detection

The monitoring system detects rebalancing through multiple indicators:

1. **State-based Detection**: Checks if group state is "PreparingRebalance", "CompletingRebalance", or "Empty"
2. **Assignment-based Detection**: Detects when members exist but have no partition assignments
3. **Distribution Tracking**: Monitors changes in partition distribution across members

### Polling and Performance

- Watch mode polls at configurable intervals (default: 5 seconds)
- Minimum recommended interval: 2 seconds (to avoid overwhelming the broker)
- Maximum recommended interval: 30 seconds (for timely detection)
- Status checks are lightweight metadata operations

### Data Structures

#### RebalanceStatus
```rust
{
    "group_id": "my-group",
    "state": "Stable",
    "members": [...],
    "total_partitions": 8,
    "is_rebalancing": false,
    "partition_distribution": {
        "consumer-1": 4,
        "consumer-2": 4
    }
}
```

#### MemberInfo
```rust
{
    "member_id": "consumer-1-12345...",
    "client_id": "consumer-1",
    "host": "/192.168.1.100",
    "assignments": {
        "topic-1": [0, 1, 2],
        "topic-2": [0]
    }
}
```

## Troubleshooting

### No Consumer Groups Found

**Problem**: `kfcli rebalance status` shows "No consumer groups found"

**Solutions**:
- Verify Kafka cluster is running and accessible
- Check that consumer groups exist: `kfcli consumer --list`
- Ensure you're connected to the correct Kafka cluster

### Continuous Rebalancing

**Problem**: Group constantly shows as rebalancing

**Possible Causes**:
1. **Session timeout too short**: Consumers can't send heartbeats fast enough
2. **Max poll interval exceeded**: Consumer processing takes too long
3. **Consumer crashes**: Check consumer logs for errors
4. **Network issues**: Intermittent connectivity problems

**Solutions**:
- Increase `session.timeout.ms` (default: 10s, try 30s)
- Increase `max.poll.interval.ms` (default: 5 minutes)
- Review consumer logs for exceptions
- Check network stability

### Uneven Partition Distribution

**Problem**: Some consumers have significantly more partitions than others

**Causes**:
- Consumers joined at different times
- Custom partition assignment strategy
- Number of partitions not divisible by number of consumers

**Solutions**:
- Wait for next rebalance (will usually even out)
- Use Range or RoundRobin assignment strategy
- Adjust number of partitions or consumers

### Watch Mode Missing Events

**Problem**: Rebalancing happened but wasn't detected in watch mode

**Causes**:
- Polling interval too long
- Rebalance completed between polls
- Network delay

**Solutions**:
- Decrease polling interval: `--interval 2`
- Check status immediately after suspected rebalance
- Review Kafka broker logs

## Best Practices

### 1. Regular Monitoring

Set up periodic status checks to catch issues early:
```bash
# Add to monitoring scripts
kfcli rebalance status > /var/log/kafka-rebalance-status.log
```

### 2. Alert on Prolonged Rebalancing

If a group is rebalancing for more than expected:
```bash
# Check every minute, alert if rebalancing > 5 minutes
# (Implement in monitoring system)
```

### 3. Use Watch Mode During Deployments

When rolling out consumer changes:
```bash
kfcli rebalance watch --group production-consumers --interval 3
```

### 4. Combine with Other Commands

Get complete picture:
```bash
# Check consumer group details
kfcli consumer --consumer my-group --pending

# Check rebalance status
kfcli rebalance status --group my-group --detailed

# Check topic details
kfcli topics details --topic my-topic
```

### 5. Document Baseline Behavior

Record normal rebalancing patterns:
- How long rebalancing typically takes
- Expected partition distribution
- Number of members
- Use as baseline for detecting anomalies

## Limitations

### Current Limitations

1. **No Historical Storage**: Events are only tracked during watch mode execution
2. **Polling-Based**: Not real-time event streaming (depends on polling interval)
3. **Memory-Based Tracking**: State comparison is in-memory only
4. **No Rebalance Metrics**: Duration and frequency not calculated

### Future Enhancements

Planned improvements (not yet implemented):
- Persistent event storage
- Rebalance duration tracking
- Frequency analysis
- Alert thresholds
- JSON output format
- Integration with monitoring systems

## Integration with Monitoring Systems

### Prometheus Integration

For Prometheus monitoring, use metrics command alongside rebalance monitoring:
```bash
kfcli metrics --format prometheus
```

### Script Integration

Example monitoring script:
```bash
#!/bin/bash
# Check for rebalancing and alert if detected

STATUS=$(kfcli rebalance status --group my-group 2>&1)

if echo "$STATUS" | grep -q "REBALANCING"; then
    echo "ALERT: Consumer group my-group is rebalancing"
    # Send alert to your monitoring system
fi
```

### Continuous Monitoring

Run watch mode as a background service:
```bash
nohup kfcli rebalance watch --group my-group --interval 5 > /var/log/rebalance-watch.log 2>&1 &
```

## See Also

- Consumer Group Management: `kfcli consumer --help`
- Cluster Metrics: `kfcli metrics --help`
- Topic Details: `kfcli topics details --help`

---

For more information and updates, see the main README and TASKS.md files.

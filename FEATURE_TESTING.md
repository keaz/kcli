# Feature Testing Guide

This document provides testing scenarios for the newly implemented Phase 2 features.

## Prerequisites

Ensure you have:
1. A running Kafka broker at `localhost:9092`
2. kfcli configured with an active environment pointing to your broker

```bash
# Configure kfcli (if not already done)
./target/release/kfcli config --setup
```

---

## P2.1: Metrics Command

### Feature: Cluster Health Metrics
Exposes comprehensive cluster metrics in multiple formats.

### Test Scenarios

#### 1. Display metrics in table format (default)
```bash
./target/release/kfcli metrics
```

**Expected Output:**
- Summary table showing: Brokers, Topics, Total Partitions, Total Messages, Consumer Groups
- Broker details table with ID, Host, Port
- Topic details table with Name, Partitions, Messages

#### 2. Display metrics in JSON format
```bash
./target/release/kfcli metrics --format json
```

**Expected Output:**
- Pretty-printed JSON with all metrics structured hierarchically
- Contains: broker_count, topic_count, total_partitions, total_messages, consumer_group_count
- Arrays: brokers[], topics[]

#### 3. Display metrics in Prometheus format
```bash
./target/release/kfcli metrics --format prometheus
```

**Expected Output:**
- Prometheus-compatible metrics with HELP and TYPE annotations
- Metrics include:
  - `kfcli_broker_count`
  - `kfcli_topic_count`
  - `kfcli_partition_count`
  - `kfcli_total_messages`
  - `kfcli_consumer_group_count`
  - `kfcli_topic_partitions{topic="..."}`
  - `kfcli_topic_messages{topic="..."}`

---

## P2.2: Producer Command

### Feature: Message Production
Produce messages to Kafka topics with support for keys, headers, and partition selection.

### Test Scenarios

#### 1. Produce a simple message
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message "Hello, Kafka!"
```

**Expected Output:**
```
Message delivered successfully to partition X at offset Y
```

#### 2. Produce a message with a key
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message "Message with key" \
  --key "user-123"
```

**Expected Output:**
```
Message delivered successfully to partition X at offset Y
```

**Verification:**
- Messages with the same key should go to the same partition

#### 3. Produce a message to a specific partition
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message "Message to partition 0" \
  --partition 0
```

**Expected Output:**
```
Message delivered successfully to partition 0 at offset Y
```

#### 4. Produce a message with headers
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message "Message with headers" \
  --headers "content-type:application/json" \
  --headers "x-correlation-id:abc-123"
```

**Expected Output:**
```
Message delivered successfully to partition X at offset Y
```

#### 5. Produce a complete message (key + headers + partition)
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message '{"event":"user.created","id":"123"}' \
  --key "user-123" \
  --partition 0 \
  --headers "event-type:user.created" \
  --headers "timestamp:2025-01-01T00:00:00Z"
```

**Expected Output:**
```
Message delivered successfully to partition 0 at offset Y
```

---

## P2.3: Batch Operations

### Feature: Batch Topic Management
Create or delete multiple topics in a single operation.

### Test Scenarios

#### 1. Batch create multiple topics
```bash
./target/release/kfcli admin batch-create-topics \
  --topics test-topic-1 \
  --topics test-topic-2 \
  --topics test-topic-3 \
  --partitions 3 \
  --replication 1
```

**Expected Output:**
```
✓ Topic 'test-topic-1' created successfully
✓ Topic 'test-topic-2' created successfully
✓ Topic 'test-topic-3' created successfully

Batch create summary: 3 succeeded, 0 failed
```

**Verification:**
```bash
./target/release/kfcli topics list
```
Should show all three new topics with 3 partitions each.

#### 2. Batch create with existing topic (partial failure)
```bash
./target/release/kfcli admin batch-create-topics \
  --topics test-topic-1 \
  --topics test-topic-4 \
  --partitions 2 \
  --replication 1
```

**Expected Output:**
```
✗ Failed to create topic 'test-topic-1': TopicAlreadyExists
✓ Topic 'test-topic-4' created successfully

Batch create summary: 1 succeeded, 1 failed
Error: Failed to create 1 topic(s): test-topic-1
```

#### 3. Batch delete multiple topics
```bash
./target/release/kfcli admin batch-delete-topics \
  --topics test-topic-1 \
  --topics test-topic-2 \
  --topics test-topic-3
```

**Expected Output:**
```
✓ Topic 'test-topic-1' deleted successfully
✓ Topic 'test-topic-2' deleted successfully
✓ Topic 'test-topic-3' deleted successfully

Batch delete summary: 3 succeeded, 0 failed
```

**Verification:**
```bash
./target/release/kfcli topics list
```
Should no longer show the deleted topics.

#### 4. Batch delete with non-existent topic (partial failure)
```bash
./target/release/kfcli admin batch-delete-topics \
  --topics test-topic-4 \
  --topics non-existent-topic
```

**Expected Output:**
```
✓ Topic 'test-topic-4' deleted successfully
✗ Failed to delete topic 'non-existent-topic': UnknownTopicOrPartition

Batch delete summary: 1 succeeded, 1 failed
Error: Failed to delete 1 topic(s): non-existent-topic
```

---

## Error Handling Tests

### 1. Invalid header format in producer
```bash
./target/release/kfcli producer \
  --topic topic-one \
  --message "test" \
  --headers "invalid-header-format"
```

**Expected Output:**
```
Error: Invalid header 'invalid-header-format'. Expected key:value
```

### 2. Empty topics list for batch operations
```bash
./target/release/kfcli admin batch-create-topics --partitions 1 --replication 1
```

**Expected Output:**
```
Error: At least one topic must be specified
```

### 3. Invalid partition count
```bash
./target/release/kfcli admin batch-create-topics \
  --topics test-topic \
  --partitions 0 \
  --replication 1
```

**Expected Output:**
```
Error: Partitions must be greater than zero
```

---

## Integration Test Workflow

Complete end-to-end workflow testing all features together:

```bash
# 1. Check initial cluster metrics
./target/release/kfcli metrics

# 2. Create test topics in batch
./target/release/kfcli admin batch-create-topics \
  --topics integration-test-1 \
  --topics integration-test-2 \
  --partitions 3 \
  --replication 1

# 3. Verify topics were created
./target/release/kfcli topics list

# 4. Produce test messages
for i in {1..10}; do
  ./target/release/kfcli producer \
    --topic integration-test-1 \
    --message "{\"id\":$i,\"data\":\"test-$i\"}" \
    --key "key-$i" \
    --headers "sequence:$i"
done

# 5. Check metrics again (should show increased message count)
./target/release/kfcli metrics --format json

# 6. Verify message production
./target/release/kfcli topics details --topic integration-test-1

# 7. Clean up test topics
./target/release/kfcli admin batch-delete-topics \
  --topics integration-test-1 \
  --topics integration-test-2

# 8. Final metrics check
./target/release/kfcli metrics
```

---

## Performance Notes

- **Metrics Command**: Fetches watermarks for all partitions across all topics. Performance scales with cluster size.
- **Producer Command**: Uses async producer with 30-second timeout. Suitable for interactive use.
- **Batch Operations**: Process multiple topics concurrently. More efficient than individual operations.

---

## Next Steps

After verifying these features work correctly:
1. Test with a production-like Kafka cluster
2. Verify Prometheus metrics can be scraped successfully
3. Test producer throughput for batch message production
4. Validate batch operations with large topic counts
5. Continue with remaining Phase 2 tasks (P2.4 onwards)

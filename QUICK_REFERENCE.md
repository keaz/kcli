# kfcli Quick Reference - New Features

## 📊 Metrics Command (P2.1)

Get cluster health metrics in multiple formats.

```bash
# Table format (default) - Human-readable
kfcli metrics

# JSON format - Machine-readable
kfcli metrics --format json

# Prometheus format - Monitoring integration
kfcli metrics --format prometheus
```

**Metrics Provided:**
- Broker count and details
- Topic count and per-topic stats
- Total partitions
- Total messages
- Consumer group count

---

## 📤 Producer Command (P2.2)

Produce messages to Kafka topics with full control.

```bash
# Basic message
kfcli producer -t topic-one -m "Hello, Kafka!"

# With message key (for partitioning)
kfcli producer -t topic-one -m "test" -k "user-123"

# To specific partition
kfcli producer -t topic-one -m "test" -p 0

# With headers
kfcli producer -t topic-one -m "test" \
  --headers "content-type:application/json" \
  --headers "x-correlation-id:abc-123"

# Complete example
kfcli producer -t topic-one \
  -m '{"event":"user.created","id":"123"}' \
  -k "user-123" \
  -p 0 \
  --headers "event-type:user.created" \
  --headers "timestamp:2025-01-01T00:00:00Z"
```

---

## 📦 Batch Operations (P2.3)

Manage multiple topics efficiently.

```bash
# Batch create topics
kfcli admin batch-create-topics \
  -t test-topic-1 \
  -t test-topic-2 \
  -t test-topic-3 \
  -p 3 \
  -r 1

# Batch delete topics
kfcli admin batch-delete-topics \
  -t test-topic-1 \
  -t test-topic-2 \
  -t test-topic-3
```

**Output Example:**
```
✓ Topic 'test-topic-1' created successfully
✓ Topic 'test-topic-2' created successfully
✓ Topic 'test-topic-3' created successfully

Batch create summary: 3 succeeded, 0 failed
```

---

## 🔍 Common Use Cases

### Monitor Cluster Health
```bash
# Quick overview
kfcli metrics

# Export to Prometheus
kfcli metrics --format prometheus > /var/lib/prometheus/kfcli.prom
```

### Send Test Messages
```bash
# Send 10 test messages
for i in {1..10}; do
  kfcli producer -t test-topic \
    -m "{\"id\":$i,\"data\":\"test-$i\"}" \
    -k "key-$i"
done
```

### Bulk Topic Setup
```bash
# Create multiple test topics
kfcli admin batch-create-topics \
  -t test-env-topic-1 \
  -t test-env-topic-2 \
  -t test-env-topic-3 \
  -p 5 -r 1

# Verify creation
kfcli topics list

# Clean up when done
kfcli admin batch-delete-topics \
  -t test-env-topic-1 \
  -t test-env-topic-2 \
  -t test-env-topic-3
```

---

## ⚡ Performance Tips

1. **Metrics Command**: On large clusters, this may take time. Use `--format json` for better performance
2. **Producer**: Designed for interactive use. For bulk production, use dedicated producer tools
3. **Batch Operations**: Much faster than individual operations for multiple topics

---

## 🐛 Troubleshooting

### Producer Command
```bash
# Error: Invalid header format
# Fix: Use key:value format
--headers "key:value"  # ✓ Correct
--headers "key=value"  # ✗ Wrong

# Error: Topic doesn't exist
# Fix: Create topic first
kfcli admin create-topic -t topic-name -p 1 -r 1
```

### Batch Operations
```bash
# Error: At least one topic must be specified
# Fix: Add topic names
kfcli admin batch-create-topics -t topic-name -p 1 -r 1

# Error: Partitions must be greater than zero
# Fix: Use positive numbers
-p 1  # ✓ Correct
-p 0  # ✗ Wrong
```

---

## 📚 Related Commands

### View Topics
```bash
kfcli topics list
kfcli topics details -t topic-name
```

### View Brokers
```bash
kfcli brokers -l
```

### View Consumer Groups
```bash
kfcli consumer -l
kfcli consumer -c group-name -p
```

---

## 🎯 Next Features (Coming Soon)

- Offset management (reset, seek)
- Message key/header display in tail
- Schema registry integration
- ACL management
- And more!

---

For detailed testing scenarios, see [FEATURE_TESTING.md](FEATURE_TESTING.md)
For implementation details, see [PHASE2_COMPLETION_SUMMARY.md](PHASE2_COMPLETION_SUMMARY.md)

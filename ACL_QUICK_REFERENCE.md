# Quick Reference: ACL Management in kfcli

## Installation Check

Verify kafka-acls.sh is available:
```bash
# Check PATH
which kafka-acls.sh

# Or set KAFKA_HOME
export KAFKA_HOME=/path/to/kafka
```

## Basic Commands

### List All ACLs
```bash
kfcli acl list
```

### Filter by Resource Type
```bash
kfcli acl list --resource-type topic
kfcli acl list --resource-type group
kfcli acl list --resource-type cluster
```

### Filter by Resource Name
```bash
kfcli acl list --name my-topic
```

### Filter by Principal
```bash
kfcli acl list --principal User:alice
```

### Combined Filters
```bash
kfcli acl list --resource-type topic --name orders --principal User:alice
```

## Create ACLs

### Grant Read Access to a Topic
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### Grant Write Access to a Topic
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:producer-app \
  --operation write \
  --permission allow
```

### Grant Consumer Group Access
```bash
kfcli acl create \
  --resource-type group \
  --resource-name my-consumer-group \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### Grant Multiple Operations (use 'all')
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:admin \
  --operation all \
  --permission allow
```

### Restrict to Specific Host
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:producer-app \
  --operation write \
  --permission allow \
  --host 192.168.1.100
```

### Use Prefixed Pattern
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name order \
  --resource-pattern prefixed \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### Deny Access
```bash
kfcli acl create \
  --resource-type topic \
  --resource-name sensitive-topic \
  --principal User:untrusted \
  --operation read \
  --permission deny
```

## Delete ACLs

### Delete Specific ACL
```bash
kfcli acl delete \
  --resource-type topic \
  --resource-name orders \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### Delete with Host Restriction
```bash
kfcli acl delete \
  --resource-type topic \
  --resource-name orders \
  --principal User:producer-app \
  --operation write \
  --permission allow \
  --host 192.168.1.100
```

## Valid Values

### Resource Types
- `topic` - Topic resources
- `group` - Consumer group resources
- `cluster` - Cluster-level resources
- `transactionalid` - Transactional ID resources
- `delegationtoken` - Delegation token resources

### Operations
- `read` - Read from resource
- `write` - Write to resource
- `create` - Create resources
- `delete` - Delete resources
- `alter` - Alter resource configuration
- `describe` - Describe resources
- `clusteraction` - Cluster-level operations
- `describeconfigs` - Describe configurations
- `alterconfigs` - Alter configurations
- `idempotentwrite` - Idempotent writes
- `all` - All operations

### Permissions
- `allow` - Grant access
- `deny` - Deny access

### Pattern Types
- `literal` - Exact match (default)
- `prefixed` - Prefix match
- `match` - Any match

## Common Use Cases

### Setup Producer
```bash
# Allow write to topic
kfcli acl create --resource-type topic --resource-name orders \
  --principal User:producer-app --operation write --permission allow

# Allow describe topic
kfcli acl create --resource-type topic --resource-name orders \
  --principal User:producer-app --operation describe --permission allow
```

### Setup Consumer
```bash
# Allow read from topic
kfcli acl create --resource-type topic --resource-name orders \
  --principal User:consumer-app --operation read --permission allow

# Allow consumer group operations
kfcli acl create --resource-type group --resource-name orders-consumers \
  --principal User:consumer-app --operation read --permission allow
```

### Setup Admin User
```bash
# Grant all operations on all topics (use prefixed pattern with empty name)
kfcli acl create --resource-type topic --resource-name "*" \
  --principal User:admin --operation all --permission allow \
  --resource-pattern literal
```

## Troubleshooting

### kafka-acls.sh not found
```bash
# Set KAFKA_HOME
export KAFKA_HOME=/path/to/kafka

# Or add to PATH
export PATH=$PATH:/path/to/kafka/bin
```

### ACLs not enabled
Check broker configuration has:
```properties
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
```

### Permission denied
Ensure you're running as a super user or have ACL management permissions.

### Invalid parameter
Check error message for valid values and correct your command.

## Testing

Verify ACL was created:
```bash
kfcli acl list --resource-type topic --name orders
```

Test with actual producer/consumer:
```bash
# As the user, try to produce/consume
kafka-console-producer --bootstrap-server localhost:9092 --topic orders
```

## Documentation

For detailed documentation, see:
- `ACL_MANAGEMENT.md` - Full guide
- `P2.5_ACL_IMPLEMENTATION_SUMMARY.md` - Implementation details
- `kfcli acl --help` - Built-in help

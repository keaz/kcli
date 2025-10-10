# ACL Management Guide

## Overview

The ACL (Access Control List) management commands in `kfcli` provide full ACL management functionality for Apache Kafka. The commands use `kafka-acls.sh` as a subprocess to interact with Kafka's ACL system, providing a user-friendly interface with parameter validation and error handling.

## Implementation

ACL management in `kfcli` (v0.2.1-alpha) uses the `kafka-acls.sh` script from the Apache Kafka distribution. This approach provides:

- **Full ACL support**: Create, list, and delete ACLs
- **Parameter validation**: Validates all parameters before execution
- **Error handling**: Clear error messages and proper error codes
- **Flexible execution**: Works with `kafka-acls.sh` in PATH or via KAFKA_HOME environment variable

## Prerequisites

Before using ACL commands, ensure:

1. **Apache Kafka installed**: `kafka-acls.sh` must be available in your PATH or set `KAFKA_HOME` environment variable
2. **Kafka cluster with ACLs enabled**: Broker must have `authorizer.class.name` configured
3. **Proper permissions**: You must have permission to manage ACLs (super user or appropriate ACL grants)

## Commands

### List ACLs

Display all ACLs or filter by specific criteria.

```bash
# List all ACLs
kfcli acl list

# Filter by resource type
kfcli acl list --resource-type topic

# Filter by resource name
kfcli acl list --name my-topic

# Filter by principal
kfcli acl list --principal User:alice

# Combine filters
kfcli acl list --resource-type topic --name my-topic --principal User:alice
```

**Output Example:**
```
Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=my-topic, patternType=LITERAL)`:
 	(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)
```

**Validation:**
- Resource type must be one of: topic, group, cluster, transactionalid, delegationtoken
- Invalid parameters will return a clear error message before attempting to execute

### Create ACL

Create a new ACL entry.

```bash
kfcli acl create \
  --resource-type topic \
  --resource-name my-topic \
  --resource-pattern literal \
  --principal User:alice \
  --operation read \
  --permission allow \
  --host "*"
```

**Parameters:**
- `--resource-type` (required): Type of resource (topic, group, cluster, transactionalid, delegationtoken)
- `--resource-name` (required): Name of the resource
- `--resource-pattern` (default: literal): Pattern type (literal, prefixed)
- `--principal` (required): Principal in the form `User:username` or `Group:groupname`
- `--operation` (required): Operation type (read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite, all)
- `--permission` (required): Permission type (allow, deny)
- `--host` (default: "*"): Host from which the principal will access the resource

**Output Example:**
```
ACL created successfully for principal 'User:alice' on topic 'my-topic'
```

**Validation:**
- All required parameters are validated before execution
- Resource type, operation, permission, and pattern type must be valid values
- Resource name and principal cannot be empty
- Detailed error messages for invalid parameters

### Delete ACL

Delete ACL entries matching the specified criteria.

```bash
kfcli acl delete \
  --resource-type topic \
  --resource-name my-topic \
  --resource-pattern literal \
  --principal User:alice \
  --operation read \
  --permission allow \
  --host "*"
```

**Parameters:** Same as create command.

**Output Example:**
```
ACL deleted successfully for principal 'User:alice' on topic 'my-topic'
```

**Note:** Delete operations use `--force` flag to skip confirmation prompts.

## Resource Types

- **topic**: Topic resources
- **group**: Consumer group resources
- **cluster**: Cluster-level resources
- **transactionalid**: Transactional ID resources
- **delegationtoken**: Delegation token resources

## Resource Pattern Types

- **literal**: Exact match of resource name
- **prefixed**: Prefix match of resource name

## Operations

- **read**: Permission to read from a resource
- **write**: Permission to write to a resource
- **create**: Permission to create resources
- **delete**: Permission to delete resources
- **alter**: Permission to alter resource configuration
- **describe**: Permission to describe resources
- **clusteraction**: Permission to perform cluster-level operations
- **describeconfigs**: Permission to describe configurations
- **alterconfigs**: Permission to alter configurations
- **idempotentwrite**: Permission for idempotent writes
- **all**: All of the above operations

## Permission Types

- **allow**: Grant the specified operation
- **deny**: Deny the specified operation

## Examples

### Grant read access to a topic

```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### Grant write access to a topic from specific host

```bash
kfcli acl create \
  --resource-type topic \
  --resource-name orders \
  --principal User:producer-app \
  --operation write \
  --permission allow \
  --host "192.168.1.100"
```

### Grant consumer group access

```bash
kfcli acl create \
  --resource-type group \
  --resource-name my-consumer-group \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

### List topic ACLs

```bash
kfcli acl list --resource-type topic
```

### Delete an ACL

```bash
kfcli acl delete \
  --resource-type topic \
  --resource-name orders \
  --principal User:consumer-app \
  --operation read \
  --permission allow
```

## Direct kafka-acls.sh Usage

If you prefer to use `kafka-acls.sh` directly, here are some common patterns:

### List all ACLs
```bash
kafka-acls.sh --bootstrap-server localhost:9092 --list
```

### Create ACL
```bash
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add \
  --allow-principal User:alice \
  --operation Read \
  --topic my-topic
```

### Delete ACL
```bash
kafka-acls.sh --bootstrap-server localhost:9092 \
  --remove \
  --allow-principal User:alice \
  --operation Read \
  --topic my-topic
```

## Testing

The ACL implementation includes comprehensive testing:

### Unit Tests
- Parameter validation (resource type, operation, permission, pattern type)
- Empty parameter detection
- Error handling for invalid inputs
- Data structure serialization

### Integration Tests
- ACL lifecycle testing (create, list, delete)
- Parameter validation with real values
- Error scenario handling
- Special character support
- Filter combination testing

Run tests with:
```bash
cargo test                           # Run all tests
cargo test --test acl_integration_tests  # Run ACL integration tests only
```

## Implementation Details

### Architecture
- Uses subprocess execution of `kafka-acls.sh` for maximum compatibility
- Parameter validation before execution prevents invalid commands
- Clear error messages guide users to fix issues
- Supports both PATH-based and KAFKA_HOME-based script discovery

### Error Handling
- `InvalidAclParameter`: Invalid parameter values
- `AclOperation`: Errors during ACL operations
- `Generic`: kafka-acls.sh not found or other system errors

### Validation Rules
- **Resource Types**: topic, group, cluster, transactionalid, delegationtoken
- **Operations**: read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite, all
- **Permissions**: allow, deny
- **Pattern Types**: literal, prefixed, match

## Future Enhancements

Future versions of `kfcli` may include:
- JSON output format for ACL listings
- ACL export/import functionality
- ACL templates for common patterns
- Integration with RBAC systems
- Direct rdkafka ACL API support (when available)

## Notes

- ACL management requires Kafka broker configuration with `authorizer.class.name` set
- The broker must be configured to use ACLs (e.g., `SimpleAclAuthorizer` or `AclAuthorizer`)
- Super users bypass ACL checks
- Always test ACL changes in a non-production environment first
- Use `--dry-run` flag with kafka-acls.sh to preview changes before applying

## Troubleshooting

### ACLs not enabled
If ACLs are not enabled on your Kafka cluster, you'll need to configure the broker with:
```properties
authorizer.class.name=kafka.security.authorizer.AclAuthorizer
```

### Permission denied errors
Ensure that:
1. ACLs are properly configured
2. The principal format is correct (e.g., `User:alice`, not just `alice`)
3. The operation matches what your application is trying to do
4. The resource name matches exactly (for literal patterns)

### Default deny
Kafka uses a default deny policy. If no ACLs match, access is denied. Ensure you've granted all necessary permissions.

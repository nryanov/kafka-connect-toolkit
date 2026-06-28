# InsertHash

*Transform · Common · [Overview](index.md)*

Calculate a hash from a selected string field and insert the result as a new field. Supported algorithms: MD5, SHA1, SHA256.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `input.fields` | Source field (may be nested) |
| `output.field` | Name of the new hash field |
| `algorithm` | Hash algorithm (default: `md5`) |

## Example

```properties
transforms=insertHash
transforms.insertHash.type=com.nryanov.kafka.connect.toolkit.transforms.InsertHash$Key
transforms.insertHash.input.fields=user.email
transforms.insertHash.output.field=email_hash
transforms.insertHash.algorithm=sha256
```

# StringToHash

*Transform · Common · [Overview](index.md)*

Replace string values with their calculated hash in hex format. Supported algorithms: MD5, SHA1, SHA256. Input must be string (or array of strings). Nested fields are supported.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.StringToHash$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.StringToHash$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated `field:algorithm` pairs |

## Example

```properties
transforms=stringToHash
transforms.stringToHash.type=com.nryanov.kafka.connect.toolkit.transforms.StringToHash$Key
transforms.stringToHash.fields=field:md5,array.nested_field:sha1,struct.nested_level.nested_field:sha256
```

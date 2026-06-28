# BytesToBase64

*Transform · Common · [Overview](index.md)*

Encode bytes fields as base64 strings.

- Use `*` to transform all bytes fields
- Specify parent fields to include all child fields

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated field paths, or `*` |

## Example

```properties
transforms=bytesToBase64Key,bytesToBase64Value

transforms.bytesToBase64Key.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Key
transforms.bytesToBase64Key.fields=field,array.inner,struct.nested.inner

transforms.bytesToBase64Value.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToBase64$Value
transforms.bytesToBase64Value.fields=*
```

# BytesToString

*Transform · Common · [Overview](index.md)*

Decode bytes fields to strings.

- Use `*` to transform all bytes fields
- Specify parent fields to include all child fields
- Configure charset for decoding

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated field paths, or `*` |
| `charset` | Character set for decoding (e.g. `UTF-8`, `WIN1251`) |

## Example

```properties
transforms=bytesToStringKey,bytesToStringValue

transforms.bytesToStringKey.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Key
transforms.bytesToStringKey.fields=field,array.inner,struct.nested.inner
transforms.bytesToStringKey.charset=WIN1251

transforms.bytesToStringValue.type=com.nryanov.kafka.connect.toolkit.transforms.BytesToString$Value
transforms.bytesToStringValue.fields=*
```

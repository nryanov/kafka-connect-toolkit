# CopyFromTo

*Transform · Common · [Overview](index.md)*

Copy specified (or all) fields from the key to the value or vice versa.

- Use `*` to copy all fields
- Specify concrete fields including nested paths: `nested.inner`

Copied fields are added at the root of the target structure. When no name mapping is given, the target name is `field.name + suffix` (suffix is configurable).

## Classes

| Part | Class |
|------|-------|
| Key → Value | `com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$KeyToValue` |
| Value → Key | `com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$ValueToKey` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated field paths, or `*` |
| `suffix` | Suffix appended when no explicit mapping (Key → Value only) |

## Example

```properties
transforms=keyToValue,valueToKey
transforms.keyToValue.type=com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$KeyToValue
transforms.keyToValue.fields=field1,field2,nested.inner
transforms.keyToValue.suffix=_key

transforms.valueToKey.type=com.nryanov.kafka.connect.toolkit.transforms.CopyFrom$ValueToKey
transforms.valueToKey.fields=*
```

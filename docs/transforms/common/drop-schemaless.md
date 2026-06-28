# DropSchemaless

*Transform · Common · [Overview](index.md)*

Drop a record if the key or value schema is null.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Value` |

## Example

```properties
transforms=dropSchemaless
transforms.dropSchemaless.type=com.nryanov.kafka.connect.toolkit.transforms.DropSchemaless$Key
```

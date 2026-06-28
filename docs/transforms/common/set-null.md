# SetNull

*Transform · Common · [Overview](index.md)*

Set the key or value to null (both payload and schema).

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.SetNull$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.SetNull$Value` |

## Example

```properties
transforms=setNullKey,setNullValue
transforms.setNullKey.type=com.nryanov.kafka.connect.toolkit.transforms.SetNull$Key
transforms.setNullValue.type=com.nryanov.kafka.connect.toolkit.transforms.SetNull$Value
```

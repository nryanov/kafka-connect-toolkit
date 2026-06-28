# DecimalAdjustScaleAndPrecision

*Transform · Common · [Overview](index.md)*

Adjust decimal precision and scale for sinks that do not support high-precision decimals, or when you need specific precision/scale values.

- Use `*` for all decimal fields
- Parent field paths include all child decimal fields

### Precision modes (`precision.mode`)

| Mode | Behavior |
|------|----------|
| `NONE` | No updates (default) |
| `IF_NOT_SET` | Set precision only when currently undefined |
| `VALUE` | Unconditionally set desired precision |
| `LIMIT` | Truncate if current precision exceeds desired |

### Scale modes (`scale.mode`)

| Mode | Behavior |
|------|----------|
| `NONE` | No updates (default) |
| `IF_NOT_SET` | Set scale only when currently undefined |
| `VALUE` | Unconditionally set desired scale |
| `LIMIT` | Truncate if current scale exceeds desired |

Additional options: `precision.undefined-value` (default `-1`), `scale.undefined-value` (default `-1`), `scale.zero-mode`, `scale.negative-mode`.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Value` |

## Example

```properties
transforms=decimalAdjustScaleAndPrecision
transforms.decimalAdjustScaleAndPrecision.type=com.nryanov.kafka.connect.toolkit.transforms.DecimalAdjustScaleAndPrecision$Key
transforms.decimalAdjustScaleAndPrecision.fields=amount,*
transforms.decimalAdjustScaleAndPrecision.precision.value=18
transforms.decimalAdjustScaleAndPrecision.precision.mode=VALUE
transforms.decimalAdjustScaleAndPrecision.scale.value=2
transforms.decimalAdjustScaleAndPrecision.scale.mode=VALUE
```

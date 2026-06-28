# CardMask

*Transform · Common · [Overview](index.md)*

Mask card numbers in text values. Uses the Luhn algorithm to detect valid card numbers before masking. Nested fields are supported.

## Classes

| Part | Class |
|------|-------|
| Key | `com.nryanov.kafka.connect.toolkit.transforms.CardMask$Key` |
| Value | `com.nryanov.kafka.connect.toolkit.transforms.CardMask$Value` |

## Configuration

| Property | Description |
|----------|-------------|
| `fields` | Comma-separated field paths |
| `masking.expose-first-count` | Digits to keep at start (default: `4`) |
| `masking.expose-last-count` | Digits to keep at end (default: `4`) |
| `masking.character` | Mask character (default: `*`) |
| `masking.separators` | Valid block separators (default: `-` and space) |
| `masking.card-number-lower-bound` | Minimum card number length (default: `15`) |
| `masking.card-number-upper-bound` | Maximum card number length (default: `16`) |

## Example

```properties
transforms=cardMaskKey
transforms.cardMaskKey.type=com.nryanov.kafka.connect.toolkit.transforms.CardMask$Key
transforms.cardMaskKey.fields=description,notes.body
transforms.cardMaskKey.masking.expose-first-count=4
transforms.cardMaskKey.masking.expose-last-count=4
```

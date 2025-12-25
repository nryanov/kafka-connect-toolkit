package com.nryanov.kafka.connect.toolkit.transforms.domain.masking;

public final class LuhnValidator {
    private static final int MODULUS = 10;

    public LuhnValidator() {}

    public boolean isValid(String input) {
        if (input == null || input.isEmpty()) {
            return false;
        }

        try {
            return calculateModulus(input) == 0;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private int calculateModulus(String input) throws IllegalArgumentException {
        var total = 0;
        var isDoubled = false;

        for (var i = input.length() - 1; i >= 0; i--) {
            var charValue = toInt(input.charAt(i), i);
            total += weightedValue(charValue, isDoubled);
            isDoubled = !isDoubled;
        }

        return total % MODULUS;
    }

    private int toInt(char character, int position) throws IllegalArgumentException {
        if (Character.isDigit(character)) {
            return Character.getNumericValue(character);
        }

        throw new IllegalArgumentException("Invalid character[" + position + "] = '" + character + "'");
    }

    private int weightedValue(int charValue, boolean isDoubled) {
        var weight = isDoubled ? 2 : 1;
        var weightedValue = charValue * weight;
        return weightedValue > 9 ? (weightedValue - 9) : weightedValue;
    }
}

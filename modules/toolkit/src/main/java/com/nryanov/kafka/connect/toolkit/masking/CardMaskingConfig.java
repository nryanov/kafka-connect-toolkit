package com.nryanov.kafka.connect.toolkit.masking;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class CardMaskingConfig {

    private final char maskingCharacter;
    private final char[] invalidSeparators;
    private final int exposeFirst;
    private final int exposeLast;

    private final String invalidSeparatorRegex;
    private final List<Pattern> cardNumberPatterns;

    public CardMaskingConfig() {
        this.maskingCharacter = 'X';
        this.invalidSeparators = new char[]{' ', '-'};
        this.exposeFirst = 6;
        this.exposeLast = 4;

        this.invalidSeparatorRegex = generateInvalidSeparatorRegex(invalidSeparators);
        this.cardNumberPatterns = new ArrayList<>();

        var cardNumberUpperBound = 16;
        var cardNumberLowerBound = 15;

        for (var cardLength = cardNumberUpperBound; cardLength >= cardNumberLowerBound; cardLength--) {
            cardNumberPatterns.add(generateCardNumberPattern(invalidSeparatorRegex, cardLength));
        }
    }

    public CardMaskingConfig(
            char maskingCharacter,
            char[] invalidSeparators,
            int exposeFirst,
            int exposeLast,
            int cardNumberUpperBound,
            int cardNumberLowerBound
    ) {
        if (cardNumberUpperBound < cardNumberLowerBound) {
            var message = String.format("Card number lower bound (%d) is greater than upper bound (%d)", cardNumberLowerBound, cardNumberUpperBound);
            throw new IllegalArgumentException(message);
        }

        this.maskingCharacter = maskingCharacter;
        this.invalidSeparators = invalidSeparators;
        this.exposeFirst = exposeFirst;
        this.exposeLast = exposeLast;

        this.invalidSeparatorRegex = generateInvalidSeparatorRegex(invalidSeparators);
        this.cardNumberPatterns = new ArrayList<>();

        for (var cardLength = cardNumberUpperBound; cardLength >= cardNumberLowerBound; cardLength--) {
            cardNumberPatterns.add(generateCardNumberPattern(invalidSeparatorRegex, cardLength));
        }
    }

    public char maskingCharacter() {
        return maskingCharacter;
    }

    public char[] invalidSeparators() {
        return invalidSeparators;
    }

    public int exposeFirst() {
        return exposeFirst;
    }

    public int exposeLast() {
        return exposeLast;
    }

    public List<Pattern> getCardNumberPatterns() {
        return cardNumberPatterns;
    }

    public String getInvalidSeparatorRegex() {
        return invalidSeparatorRegex;
    }

    private static Pattern generateCardNumberPattern(String invalidSeparatorRegex, int cardNumberLength) {
        var regex = String.format("(?:\\d%s*?){%d}", invalidSeparatorRegex, cardNumberLength);
        return Pattern.compile(regex);
    }

    private static String generateInvalidSeparatorRegex(char[] invalidSeparators) {
        var invalidSeparatorsSet = new HashSet<Character>();

        for (var separator : invalidSeparators) {
            invalidSeparatorsSet.add(separator);
        }

        return buildSeparatorRegexString(invalidSeparatorsSet);
    }

    private static String buildSeparatorRegexString(Set<Character> separators) {
        var regexBuilder = new StringBuilder();

        regexBuilder.append('[');

        if (separators.contains(' ')) {
            regexBuilder.append(' ');
            separators.remove(' ');
        }

        for (var separator : separators) {
            regexBuilder.append(separator);
        }

        regexBuilder.append(']');

        return regexBuilder.toString();
    }
}

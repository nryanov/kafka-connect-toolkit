package com.nryanov.kafka.connect.toolkit.transforms.masking;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/*
Source: https://github.com/chrislmy/credit-card-sanitizer
 */
public class CardMaskingService {
    private final CardMaskingConfig cardMaskingConfig;

    private final String invalidSeparatorRegex;
    private final List<Pattern> cardNumberPatterns;
    private final LuhnValidator validator;

    public CardMaskingService(CardMaskingConfig cardMaskingConfig) {
        this.cardMaskingConfig = cardMaskingConfig;
        this.invalidSeparatorRegex = cardMaskingConfig.getInvalidSeparatorRegex();
        this.validator = new LuhnValidator();
        this.cardNumberPatterns = cardMaskingConfig.getCardNumberPatterns();
    }

    public String maskCards(String input) {
        if (input == null) {
            return null;
        }

        var validCardNumberMatches = getValidCardNumberMatches(input);
        return performSanitization(input, validCardNumberMatches);
    }

    private String performSanitization(String input, List<String> foundCardNumbers) {
        for (var cardNumber : foundCardNumbers) {
            input = input.replace(cardNumber, maskString(cardNumber));
        }

        return input;
    }

    private String maskString(String cardNumberMatch) {
        var cleanedCardNumberMatch = removeSeparators(cardNumberMatch);
        var charArray = cleanedCardNumberMatch.toCharArray();

        for (var i = cardMaskingConfig.exposeFirst(); i < cleanedCardNumberMatch.length() - cardMaskingConfig.exposeLast(); i++) {
            charArray[i] = cardMaskingConfig.maskingCharacter();
        }

        return String.valueOf(charArray);
    }

    private List<String> getValidCardNumberMatches(String input) {
        try {
            return findCardNumbers(input);
        } catch (PatternSyntaxException e) {
            var message = String.format("Invalid separators provided: %s", cardMaskingConfig.invalidSeparators());
            throw new IllegalArgumentException(message);
        }
    }

    private List<String> findCardNumbers(String input) {
        var matches = new ArrayList<String>();

        for (var pattern : cardNumberPatterns) {
            var matcher = pattern.matcher(input);

            while (matcher.find()) {
                matches.add(matcher.group());
            }
        }

        return filterValidCreditCardNumbers(matches);
    }

    private List<String> filterValidCreditCardNumbers(List<String> cardNumbers) {
        return cardNumbers
                .stream()
                .filter(this::isValidCreditCardNumber)
                .collect(Collectors.toList());
    }

    private boolean isValidCreditCardNumber(String cardNumber) {
        var cleanedCardNumber = removeSeparators(cardNumber);
        return validator.isValid(cleanedCardNumber);
    }

    private String removeSeparators(String cardNumber) {
        return cardNumber.replaceAll(invalidSeparatorRegex, "");
    }
}

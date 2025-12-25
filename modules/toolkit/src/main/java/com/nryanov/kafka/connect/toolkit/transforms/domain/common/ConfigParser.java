package com.nryanov.kafka.connect.toolkit.transforms.domain.common;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ConfigParser {
    public static Set<String> parseCommaSeparatedSingleValues(AbstractConfig config, String name) {
        var input = Objects.requireNonNullElse(config.getString(name), "");
        return parseCommaSeparatedSingleValues(input);
    }

    public static Set<String> parseCommaSeparatedSingleValues(String value) {
        var result = new HashSet<String>();
        Arrays
                .asList(value.split(","))
                .forEach(it -> {
                    if (!it.isBlank()) {
                        result.add(it);
                    }
                });

        return result;
    }

    public static LinkedHashSet<String> parseCommaSeparatedSingleValuesPreserveOrder(String value) {
        var result = new LinkedHashSet<String>();
        Arrays
                .asList(value.split(","))
                .forEach(it -> {
                    if (!it.isBlank()) {
                        result.add(it);
                    }
                });

        return result;
    }

    public static Map<String, String> parseCommaSeparatedPairs(AbstractConfig config, String name) {
        var result = new HashMap<String, String>();
        Arrays.stream(Objects.requireNonNullElse(config.getString(name), "")
                        .split(","))
                .forEach(it -> {
                    var pair = it.split(":");

                    if (pair.length == 2) {
                        var from = pair[0];
                        var to = pair[1];

                        result.put(from, to);
                    }
                });

        return result;
    }
}

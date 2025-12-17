package com.nryanov.kafka.connect.toolkit.transforms.common;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ConfigParser {
    public static void parseCommaSeparatedSingleValues(AbstractConfig config, String name, Set<String> target) {
        Arrays
                .asList(Objects.requireNonNullElse(config.getString(name), "").split(","))
                .forEach(it -> {
                    if (!it.isBlank()) {
                        target.add(it);
                    }
                });
    }

    public static void parseCommaSeparatedPairs(AbstractConfig config, String name, Map<String, String> target) {
        Arrays.stream(Objects.requireNonNullElse(config.getString(name), "")
                        .split(","))
                .forEach(it -> {
                    var pair = it.split(":");

                    if (pair.length == 2) {
                        var from = pair[0];
                        var to = pair[1];

                        target.put(from, to);
                    }
                });
    }
}

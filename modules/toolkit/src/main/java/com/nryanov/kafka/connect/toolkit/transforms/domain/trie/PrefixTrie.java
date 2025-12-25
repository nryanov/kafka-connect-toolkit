package com.nryanov.kafka.connect.toolkit.transforms.domain.trie;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Simple prefix trie implementation which allows specify not only concrete field names of leafs,
 * but also just specify parent nodes to perform transform on all child nodes.
 * As input this trie expect field names in format {part}({[.]subpart}*)
 */
public final class PrefixTrie {
    final Map<String, PrefixTrie> trie = new HashMap<>();

    public PrefixTrie() {
    }

    public void build(Collection<String> includeFields) {
        includeFields.forEach(it -> {
            var splits = it.split("[.]");

            var current = this;
            for (var split : splits) {
                var next = current.trie.get(split);
                if (next == null) {
                    next = new PrefixTrie();
                    current.trie.put(split, next);
                }

                current = next;
            }
        });
    }

    public boolean isEmpty() {
        return trie.isEmpty();
    }

    public boolean shouldInclude(String value) {
        var splits = value.split("[.]");

        var current = this;
        var i = 0;

        for (; i < splits.length && current.trie.containsKey(splits[i]); i++) {
            current = current.trie.get(splits[i]);
        }

        // specific field should be included
        if (i == splits.length) {
            return true;
        }

        // all child fields should be included or this filed and it's child should be excluded
        return current.trie.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        PrefixTrie that = (PrefixTrie) o;
        return Objects.equals(trie, that.trie);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(trie);
    }
}

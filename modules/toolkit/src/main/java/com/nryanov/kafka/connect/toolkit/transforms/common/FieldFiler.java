package com.nryanov.kafka.connect.toolkit.transforms.common;

import com.nryanov.kafka.connect.toolkit.transforms.trie.PrefixTrie;

import java.util.Collection;

public sealed interface FieldFiler {
    boolean enabled();

    boolean shouldApply(String field);

    record None() implements FieldFiler {
        @Override
        public boolean enabled() {
            return false;
        }

        @Override
        public boolean shouldApply(String field) {
            return false;
        }
    }

    record All() implements FieldFiler {
        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public boolean shouldApply(String field) {
            return true;
        }
    }

    record Subset(PrefixTrie trie) implements FieldFiler {
        public Subset(Collection<String> fields) {
            this(build(fields));
        }

        @Override
        public boolean enabled() {
            return true;
        }

        @Override
        public boolean shouldApply(String field) {
            return trie.shouldInclude(field);
        }

        private static PrefixTrie build(Collection<String> fields) {
            var trie = new PrefixTrie();
            trie.build(fields);

            return trie;
        }
    }
}

package com.nryanov.kafka.connect.toolkit.transforms.common;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Map;

public class SchemaCopyUtil {
    public static SchemaBuilder copySchemaBasics(Schema source) {
        var builder = SchemaUtil.copySchemaBasics(source);

        if (source.isOptional()) {
            builder.optional();
        }

        return builder;
    }

    public static SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder builder) {
        builder.name(source.name());
        builder.version(source.version());
        builder.doc(source.doc());

        final Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        if (source.isOptional()) {
            builder.optional();
        }

        return builder;
    }
}

package com.nryanov.kafka.connect.toolkit.transforms.domain.hash;

import java.nio.charset.StandardCharsets;

// src: https://stackoverflow.com/a/9855338
// compatible with: https://github.com/apache/commons-codec/blob/1129c6792df7ca92e57f63b6455007c1382a89a2/src/main/java/org/apache/commons/codec/binary/Hex.java#L208-L214
public class Hex {
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public static String bytesToHex(byte[] bytes) {
        var hexChars = new byte[bytes.length * 2];

        for (var j = 0; j < bytes.length; j++) {
            var v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }
}

package com.nryanov.kafka.connect.toolkit.fixtures.jars;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarBuilder {
    private final static String DEFAULT_REPLACEMENT_TARGET = "build/classes/";

    public static void create(String outputPath, String classesLocation) throws IOException {
        var manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        var target = new JarOutputStream(new FileOutputStream(outputPath), manifest);
        add(new File(classesLocation), target);
        target.close();
    }

    private static void add(File source, JarOutputStream target) throws IOException {
        var name = source.getPath().replace("\\", "/").replace(DEFAULT_REPLACEMENT_TARGET, "");
        if (source.isDirectory()) {
            if (!name.endsWith("/")) {
                name += "/";
            }
            var entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            target.closeEntry();
            for (var nestedFile : source.listFiles()) {
                add(nestedFile, target);
            }
        }
        else {
            var entry = new JarEntry(name);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            try (var in = new BufferedInputStream(new FileInputStream(source))) {
                var buffer = new byte[1024];
                while (true) {
                    var count = in.read(buffer);
                    if (count == -1)
                        break;
                    target.write(buffer, 0, count);
                }
                target.closeEntry();
            }
        }
    }
}

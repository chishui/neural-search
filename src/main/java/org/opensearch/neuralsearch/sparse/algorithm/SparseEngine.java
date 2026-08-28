/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.Arrays;

public enum SparseEngine {
    LUCENE("lucene"),
    NATIVE("native");

    private final String name;
    // 102: back to seismic_sq, now that the quantized index is mmap-able too. Each of
    // these switches the payload layout, and nothing versions the file itself, so the
    // bump is what keeps a 101-era float file from being parsed as a quantized one --
    // the version is part of the engine file name.
    private final String version = "102";
    private final String extension = ".nsparse";

    SparseEngine(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String version() {
        return version;
    }

    public String extension() {
        return extension;
    }

    public static SparseEngine fromName(String name) {
        return Arrays.stream(values()).filter(e -> e.name.equalsIgnoreCase(name)).findFirst().orElse(null);
    }

    public static final SparseEngine DEFAULT = LUCENE;
}

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
    private final String version = "101";
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

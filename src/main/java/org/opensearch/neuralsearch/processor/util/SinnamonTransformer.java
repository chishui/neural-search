/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * SinnamonTransformer provides functionality to transform high-dimensional vectors
 * into lower-dimensional sketches using the Weak Sinnamon sketch technique.
 */
public class SinnamonTransformer {

    private int[] randomMapping;
    public static final int SINNAMON_SKETCH_SIZE = 1024;
    private static final String SINNAMON_MAPPING_RESOURCE = "sinnamon_mapping.bin";

    // Instance is created at class loading time
    private static volatile SinnamonTransformer INSTANCE;

    /**
     * Default constructor for serialization
     */
    private SinnamonTransformer() {}

    public static SinnamonTransformer getInstance() {
        if (INSTANCE == null) {
            synchronized (SinnamonTransformer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SinnamonTransformer();
                }
            }
        }
        return INSTANCE;
    }

    public void initialize() {
        loadRandomMapping();
    }

    /**
     * Loads random mapping from a file in the temporary directory
     */
    private void loadRandomMapping() {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                String tempDir = System.getProperty("java.io.tmpdir");
                File file = new File(tempDir, SINNAMON_MAPPING_RESOURCE);

                if (!file.exists() || !file.canRead()) {
                    System.err.println("Sinnamon mapping file doesn't exist or isn't readable: {}" + file.getAbsolutePath());
                    return null;
                }

                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] assignmentBytes = fis.readAllBytes();
                    ByteBuffer assignmentBuffer = ByteBuffer.wrap(assignmentBytes).order(ByteOrder.nativeOrder());

                    randomMapping = new int[assignmentBytes.length / 4];

                    for (int i = 0; i < randomMapping.length; i++) {
                        randomMapping[i] = assignmentBuffer.getInt(i * 4);
                    }

                    System.out.println("Successfully loaded sinnamon mapping file");
                } catch (IOException e) {
                    System.err.println("Error reading sinnamon mapping file: " + e.getMessage());
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            System.err.println("Security error while loading sinnamon mapping file: " + e.getException());
        }
    }

    /**
     * Convert a single vector into its Weak Sinnamon sketch representation
     *
     * @param tokens: Input map of tokens
     * @return Sketch vector
     */
    public float[] convertSketchVector(Map<String, Float> tokens) {
        // Initialize sketch vector
        int sketchIdx;
        double value;
        float[] sketch = new float[SINNAMON_SKETCH_SIZE];

        for (Map.Entry<String, Float> entry : tokens.entrySet()) {
            value = ((Number) entry.getValue()).floatValue();
            sketchIdx = randomMapping[Integer.parseInt(entry.getKey())];
            sketch[sketchIdx] = Math.max(sketch[sketchIdx], (float) value);
        }

        return sketch;
    }
}

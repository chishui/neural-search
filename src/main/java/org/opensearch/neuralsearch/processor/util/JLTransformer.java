/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static org.apache.lucene.util.VectorUtil.dotProduct;

public class JLTransformer {
    private float[][] projectionMatrix;

    private static final String PROJECTION_MATRIX_RESOURCE = "jl_transformer.bin"; // default transformer path
    private static final int INPUT_DIMENSION = 30109; // Input dimension
    private static final int OUTPUT_DIMENSION = 1024;  // Output dimension

    // Instance is created at class loading time
    private static volatile JLTransformer INSTANCE;

    private JLTransformer() {}

    public static JLTransformer getInstance() {
        if (INSTANCE == null) {
            synchronized (JLTransformer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new JLTransformer();
                }
            }
        }
        return INSTANCE;
    }

    public void initialize() {
        loadProjectionMatrix();
    }

    private void loadProjectionMatrix() {
        try {
            // Use AccessController to perform privileged file operations
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                String tempDir = System.getProperty("java.io.tmpdir");
                File file = new File(tempDir, PROJECTION_MATRIX_RESOURCE);

                if (!file.exists() || !file.canRead()) {
                    System.err.println("Projection matrix file doesn't exist or isn't readable: " + file.getAbsolutePath());
                    projectionMatrix = new float[0][0];
                    return null;
                }

                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] matrixBytes = fis.readAllBytes();
                    ByteBuffer matrixBuffer = ByteBuffer.wrap(matrixBytes).order(ByteOrder.nativeOrder());

                    // Verify the size of file
                    int expectedSize = OUTPUT_DIMENSION * INPUT_DIMENSION * 4; // 4 bytes per float
                    if (matrixBytes.length != expectedSize) {
                        System.err.println("Warning: File size doesn't match expected dimensions!");
                        System.err.println("Expected: " + expectedSize + " bytes");
                        System.err.println("Actual: " + matrixBytes.length + " bytes");
                    }

                    // Initialize the projection matrix
                    projectionMatrix = new float[OUTPUT_DIMENSION][INPUT_DIMENSION];

                    // Read matrix data
                    for (int i = 0; i < OUTPUT_DIMENSION; i++) {
                        for (int j = 0; j < INPUT_DIMENSION; j++) {
                            projectionMatrix[i][j] = matrixBuffer.getFloat((i * INPUT_DIMENSION + j) * 4);
                        }
                    }

                    System.out.println("Successfully loaded projection matrix: " + OUTPUT_DIMENSION + " x " + INPUT_DIMENSION);
                } catch (IOException e) {
                    System.err.println("Error reading projection matrix file: " + e.getMessage());
                    projectionMatrix = new float[0][0];
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            System.err.println("Error jl transformer data: " + e.getMessage());
            projectionMatrix = new float[0][0];
        } catch (OutOfMemoryError e) {
            System.err.println("Not enough memory to load projection matrix: " + e.getMessage());
            projectionMatrix = new float[0][0];
        }
    }

    public float[] convertSketchVector(float[] vector) {
        if (projectionMatrix.length == 0 || vector.length != INPUT_DIMENSION) {
            throw new IllegalArgumentException("Invalid dimensions for projection");
        }

        float[] result = new float[OUTPUT_DIMENSION];

        for (int i = 0; i < projectionMatrix.length; i++) {
            // Each row of the matrix is multiplied with the vector (dot product)
            result[i] = dotProduct(projectionMatrix[i], vector);
        }

        return result;
    }
}

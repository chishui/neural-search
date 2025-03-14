/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import java.util.stream.Collectors;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class JLTransformer {
    private float[][] projectionMatrix;

    private static final String PROJECTION_MATRIX_RESOURCE = "transformer.bin"; // default transformer path
    private static final int INPUT_DIMENSION = 30109; // Input dimension
    private static final int OUTPUT_DIMENSION = 1024;  // Output dimension
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public JLTransformer() {
        initialize();
    }

    private void initialize() {
        loadProjectionMatrix();
    }

    private void loadProjectionMatrix() {
        String tempDir = System.getProperty("java.io.tmpdir");
        File file = new File(tempDir, PROJECTION_MATRIX_RESOURCE);

        if (!file.exists() || !file.canRead()) {
            System.err.println("Projection matrix file doesn't exist or isn't readable: " + file.getAbsolutePath());
            projectionMatrix = new float[0][0];
            return;
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

            System.out.println(
                    "Successfully loaded projection matrix: " + OUTPUT_DIMENSION + " x " + INPUT_DIMENSION
            );
        } catch (IOException e) {
            System.err.println("Error reading projection matrix file: " + e.getMessage());
            projectionMatrix = new float[0][0];
        } catch (OutOfMemoryError e) {
            System.err.println("Not enough memory to load projection matrix: " + e.getMessage());
            projectionMatrix = new float[0][0];
        }
    }

    public float[][] getProjectionMatrix() {
        return projectionMatrix;
    }

    public float[] project(float[] vector) {
        if (projectionMatrix.length == 0 || vector.length != INPUT_DIMENSION) {
            throw new IllegalArgumentException("Invalid dimensions for projection");
        }

        float[] result = new float[OUTPUT_DIMENSION];

        for (int i = 0; i < OUTPUT_DIMENSION; i++) {
            float sum = 0;
            int j = 0;
            int upperBound = SPECIES.loopBound(INPUT_DIMENSION);

            // Vectorized computation
            var sumVector = FloatVector.zero(SPECIES);
            for (; j < upperBound; j += SPECIES.length()) {
                var v = FloatVector.fromArray(SPECIES, vector, j);
                var m = FloatVector.fromArray(SPECIES, projectionMatrix[i], j);
                sumVector = sumVector.add(v.mul(m));
            }
            sum += sumVector.reduceLanes(VectorOperators.ADD);

            // Handle remaining elements
            for (; j < INPUT_DIMENSION; j++) {
                sum += projectionMatrix[i][j] * vector[j];
            }

            result[i] = sum;
        }

        return result;
    }
}
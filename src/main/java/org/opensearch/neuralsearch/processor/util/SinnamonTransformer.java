/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.io.*;
import java.util.Random;

/**
 * SinnamonTransformer provides functionality to transform high-dimensional vectors
 * into lower-dimensional sketches using the Weak Sinnamon sketch technique.
 */
public class SinnamonTransformer {
    private int sketchSize;
    private int[] randomMapping;

    /**
     * Default constructor for serialization
     */
    public SinnamonTransformer() {
    }

    /**
     * Initialize a new SinnamonTransformer with the specified sketch size
     *
     * @param sketchSize: Size of the sketch dimensions
     * @param originalDimension: Original dimension of vectors to be sketched
     */
    public SinnamonTransformer(int sketchSize, int originalDimension) {
        this.sketchSize = sketchSize;
        this.randomMapping = new int[originalDimension];
        Random random = new Random();
        for (int i = 0; i < originalDimension; i++) {
            this.randomMapping[i] = random.nextInt(sketchSize);
        }
    }

    /**
     * Initialize a SinnamonTransformer with a predefined mapping
     *
     * @param sketchSize: Size of the sketch dimensions
     * @param randomMapping: Predefined mapping from original dimensions to sketch dimensions
     */
    public SinnamonTransformer(int sketchSize, int[] randomMapping) {
        this.sketchSize = sketchSize;
        this.randomMapping = randomMapping;
    }

    /**
     * Save the random mapping to a file
     *
     * @param filepath: Path to save the mapping
     * @throws IOException: If an I/O error occurs
     */
    public void saveRandomMapping(String filepath) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filepath))) {
            out.writeObject(randomMapping);
        }
    }

    /**
     * Load a SinnamonTransformer from a saved mapping file
     *
     * @param sketchSize: Size of the sketch
     * @param mappingFilepath: Path to the saved mapping
     * @return SinnamonTransformer with loaded mapping
     * @throws IOException If an I/O error occurs
     * @throws ClassNotFoundException If the class of the serialized object cannot be found
     */
    public static SinnamonTransformer loadFromMapping(int sketchSize, String mappingFilepath)
            throws IOException, ClassNotFoundException {
        int[] randomMapping;
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(mappingFilepath))) {
            randomMapping = (int[]) in.readObject();
        }
        return new SinnamonTransformer(sketchSize, randomMapping);
    }

    /**
     * Convert a single vector into its Weak Sinnamon sketch representation
     *
     * @param vector: Input vector
     * @return Sketch vector
     */
    public float[] sketchVector(float[] vector) {
        // Initialize sketch vector
        float[] sketch = new float[sketchSize];

        // Process non-zero elements
        for (int idx = 0; idx < vector.length; idx++) {
            if (vector[idx] != 0) {
                int sketchIdx = randomMapping[idx];
                float value = vector[idx];

                // Update sketch with maximum value
                sketch[sketchIdx] = Math.max(sketch[sketchIdx], value);
            }
        }

        return sketch;
    }

    /**
     * Convert a matrix of vectors into sketches
     *
     * @param matrix: Input matrix where each row is a vector
     * @return Matrix of sketches in sparse format
     */
    public SparseMatrix sketchMatrix(float[][] matrix) {
        int nDocs = matrix.length;
        float[][] sketches = new float[nDocs][sketchSize];

        for (int i = 0; i < nDocs; i++) {
            sketches[i] = sketchVector(matrix[i]);
        }

        return convertToSparseMatrix(sketches);
    }

    /**
     * Convert a dense matrix to sparse matrix representation (CSR format)
     *
     * @param matrix: Dense matrix
     * @return Sparse matrix representation
     */
    private SparseMatrix convertToSparseMatrix(float[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;

        // Count non-zero elements
        int nnz = 0;
        for (float[] row : matrix) {
            for (float val : row) {
                if (val != 0) nnz++;
            }
        }

        // Create CSR format
        float[] data = new float[nnz];
        int[] indices = new int[nnz];
        int[] indptr = new int[rows + 1];

        int pos = 0;
        for (int i = 0; i < rows; i++) {
            indptr[i] = pos;
            for (int j = 0; j < cols; j++) {
                if (matrix[i][j] != 0) {
                    data[pos] = matrix[i][j];
                    indices[pos] = j;
                    pos++;
                }
            }
        }
        indptr[rows] = pos;

        return new SparseMatrix(rows, cols, indptr, indices, data);
    }

    /**
     * Get the sketch size
     * @return Sketch size
     */
    public int getSketchSize() {
        return sketchSize;
    }

    /**
     * Get the random mapping
     * @return Random mapping array
     */
    public int[] getRandomMapping() {
        return randomMapping;
    }

    /**
     * Set the sketch size
     * @param sketchSize: New sketch size
     */
    public void setSketchSize(int sketchSize) {
        this.sketchSize = sketchSize;
    }

    /**
     * Set the random mapping
     * @param randomMapping: New random mapping
     */
    public void setRandomMapping(int[] randomMapping) {
        this.randomMapping = randomMapping;
    }

    /**
     * Simple sparse matrix representation (CSR format)
     */
    public static class SparseMatrix implements Serializable {
        private static final long serialVersionUID = 1L;

        private int rows;
        private int cols;
        private int[] indptr;
        private int[] indices;
        private float[] data;

        /**
         * Default constructor for serialization
         */
        public SparseMatrix() {
        }

        /**
         * Create a sparse matrix in CSR format
         *
         * @param rows: Number of rows
         * @param cols: Number of columns
         * @param indptr: Row pointers
         * @param indices: Column indices
         * @param data: Non-zero values
         */
        public SparseMatrix(int rows, int cols, int[] indptr, int[] indices, float[] data) {
            this.rows = rows;
            this.cols = cols;
            this.indptr = indptr;
            this.indices = indices;
            this.data = data;
        }

        // Getters and setters
        public int getRows() { return rows; }
        public void setRows(int rows) { this.rows = rows; }

        public int getCols() { return cols; }
        public void setCols(int cols) { this.cols = cols; }

        public int[] getIndptr() { return indptr; }
        public void setIndptr(int[] indptr) { this.indptr = indptr; }

        public int[] getIndices() { return indices; }
        public void setIndices(int[] indices) { this.indices = indices; }

        public float[] getData() { return data; }
        public void setData(float[] data) { this.data = data; }

        /**
         * Convert sparse matrix to dense array representation
         *
         * @return Dense 2D array
         */
        public float[][] toDenseArray() {
            float[][] result = new float[rows][cols];

            for (int i = 0; i < rows; i++) {
                for (int j = indptr[i]; j < indptr[i+1]; j++) {
                    result[i][indices[j]] = data[j];
                }
            }

            return result;
        }
    }

    /**
     * Utility class for reading and writing sparse matrices
     */
    public static class SparseMatrixIO {
        /**
         * Read a sparse CSR matrix from a file
         *
         * @param filePath: Path to the file
         * @return Dense matrix representation
         * @throws IOException If an I/O error occurs
         */
        public static float[][] readSparseCSRMatrix(String filePath) throws IOException {
            // This is a placeholder - implement according to your file format
            // For demonstration, returning a small example matrix
            return new float[10][30109];
        }

        /**
         * Save a sparse matrix to a file
         *
         * @param matrix: Matrix to save
         * @param filePath: Path to save the matrix
         * @throws IOException If an I/O error occurs
         */
        public static void saveSparseMatrix(SparseMatrix matrix, String filePath) throws IOException {
            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filePath))) {
                out.writeObject(matrix);
            }
        }

        /**
         * Load a sparse matrix from a file
         *
         * @param filePath: Path to the file
         * @return Loaded sparse matrix
         * @throws IOException If an I/O error occurs
         * @throws ClassNotFoundException If the class of the serialized object cannot be found
         */
        public static SparseMatrix loadSparseMatrix(String filePath) throws IOException, ClassNotFoundException {
            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filePath))) {
                return (SparseMatrix) in.readObject();
            }
        }
    }

}

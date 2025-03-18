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
import java.util.Arrays;
import java.util.Objects;

import static org.apache.lucene.util.VectorUtil.dotProduct;

/**
 * Helper class with cluster representatives and assignments. Often used to getTopClusters from a query sketch.
 */
public class DocumentClusterManager {

    private int totalDocCounts; // total number of documents within the index
    private int[] clusterDocCounts; // number of documents across each cluster, both jl and sinnamon share the same assignment
    private float[][] jlClusterRepresentatives; // an array of jl sketch vectors indicating the center of each cluster
    private float[][] sinnamonClusterRepresentatives; // an array of sinnamon sketch vectors indicating the center of each cluster

    // Resource paths relative to classpath
    public static final int SKETCH_SIZE = 1024;
    public static final int CLUSTER_NUM = 11896;

    private static final String CLUSTER_ASSIGNMENT_RESOURCE = "assignment.bin";
    private static final String JL_CLUSTER_REPRESENTATIVE_RESOURCE = "jl_representative.bin";
    private static final String SINNAMON_CLUSTER_REPRESENTATIVE_RESOURCE = "sinnamon_representative.bin";

    // Instance is created at class loading time
    private static volatile DocumentClusterManager INSTANCE;

    private DocumentClusterManager() {}

    private void initialize() {
        clusterDocCounts = loadClusterAssignment();
        jlClusterRepresentatives = loadClusterRepresentative(JL_CLUSTER_REPRESENTATIVE_RESOURCE);
        sinnamonClusterRepresentatives = loadClusterRepresentative(SINNAMON_CLUSTER_REPRESENTATIVE_RESOURCE);
    }

    // lazy load
    public static DocumentClusterManager getInstance() {
        if (INSTANCE == null) {
            synchronized (DocumentClusterManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DocumentClusterManager();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Loads cluster assignment data from a file in the temporary directory
     */
    private int[] loadClusterAssignment() {
        clusterDocCounts = new int[CLUSTER_NUM];
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                String tempDir = System.getProperty("java.io.tmpdir");
                File file = new File(tempDir, DocumentClusterManager.CLUSTER_ASSIGNMENT_RESOURCE);
                if (!file.exists() || !file.canRead()) {
                    System.err.println("Cluster assignment file doesn't exist or isn't readable: {}" + file.getAbsolutePath());
                    return null;
                }

                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] assignmentBytes = fis.readAllBytes();
                    ByteBuffer assignmentBuffer = ByteBuffer.wrap(assignmentBytes).order(ByteOrder.nativeOrder());

                    totalDocCounts = assignmentBytes.length / 4;

                    for (int i = 0; i < totalDocCounts; i++) {
                        int clusterId = assignmentBuffer.getInt(i * 4);
                        clusterDocCounts[clusterId] += 1;
                    }

                    System.out.println(
                        "Successfully loaded cluster assignment data: {} clusters with {} total documents"
                            + clusterDocCounts.length
                            + " "
                            + totalDocCounts
                    );
                } catch (IOException e) {
                    System.err.println("Error reading cluster assignment file: " + e.getMessage());
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            System.err.println("Security error while loading cluster assignment data: " + e.getException());
        }
        return clusterDocCounts;
    }

    /**
     * Loads cluster representative data from a file in the temporary directory
     *
     */
    public float[][] loadClusterRepresentative(String representativeResourcePath) {
        float[][] clusterRepresentatives = new float[CLUSTER_NUM][SKETCH_SIZE];
        try {
            // Use AccessController to perform privileged file operations
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                String tempDir = System.getProperty("java.io.tmpdir");
                File file = new File(tempDir, representativeResourcePath);
                if (!file.exists() || !file.canRead()) {
                    System.err.println("Cluster assignment file doesn't exist or isn't readable: {}" + file.getAbsolutePath());
                    return null;
                }

                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] representativeBytes = fis.readAllBytes();
                    ByteBuffer representativeBuffer = ByteBuffer.wrap(representativeBytes).order(ByteOrder.nativeOrder());

                    // Verify file size matches expected dimensions
                    if (representativeBytes.length != CLUSTER_NUM * SKETCH_SIZE * 4) {
                        System.err.println("Warning: File size doesn't match expected dimensions!");
                        System.err.println("Expected: " + (CLUSTER_NUM * SKETCH_SIZE * 4) + " bytes");
                        System.err.println("Actual: " + representativeBytes.length + " bytes");
                    }

                    for (int i = 0; i < CLUSTER_NUM; i++) {
                        for (int j = 0; j < SKETCH_SIZE; j++) {
                            clusterRepresentatives[i][j] = representativeBuffer.getFloat((i * SKETCH_SIZE + j) * 4);
                        }
                    }

                    System.out.println(
                        "Successfully loaded cluster assignment data: {} clusters with {} total documents"
                            + clusterRepresentatives.length
                            + " "
                            + totalDocCounts
                    );
                } catch (IOException e) {
                    System.err.println("Error reading cluster assignment file: " + e.getMessage());
                    clusterRepresentatives = new float[0][0];
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            System.err.println("Error loading cluster representative data: " + e.getMessage());
            clusterRepresentatives = new float[0][0];
        } catch (OutOfMemoryError e) {
            System.err.println("Not enough memory to load cluster representative data: " + e.getMessage());
            clusterRepresentatives = new float[0][0];
        }
        return clusterRepresentatives;
    }

    private float[] computeDotProductWithClusterRepresentatives(float[] querySketch, String sketchType) {
        float[] dotProductWithClusterRepresentatives = new float[SKETCH_SIZE];
        if (Objects.equals(sketchType, "Sinnamon")) {
            for (int i = 0; i < SKETCH_SIZE; i += 1) {
                dotProductWithClusterRepresentatives[i] = dotProduct(querySketch, sinnamonClusterRepresentatives[i]);
            }
            return dotProductWithClusterRepresentatives;
        }
        for (int i = 0; i < SKETCH_SIZE; i += 1) {
            dotProductWithClusterRepresentatives[i] = dotProduct(querySketch, jlClusterRepresentatives[i]);
        }
        return dotProductWithClusterRepresentatives;
    }

    public Integer[] getTopClusters(float[] querySketch, float ratio, String sketchType) throws IllegalArgumentException {
        if (ratio > 1 || ratio <= 0) {
            throw new IllegalArgumentException("ratio should be in (0, 1]");
        }
        float[] dotProductWithClusterRepresentatives = computeDotProductWithClusterRepresentatives(querySketch, sketchType);

        Integer[] indices = new Integer[dotProductWithClusterRepresentatives.length];
        for (int i = 0; i < dotProductWithClusterRepresentatives.length; i++) {
            indices[i] = i;
        }

        // Sort indices by dot product values in descending order
        Arrays.sort(indices, (a, b) -> Float.compare(dotProductWithClusterRepresentatives[b], dotProductWithClusterRepresentatives[a]));

        // Calculate how many documents we need to cover based on the ratio
        int documentsToExamine = (int) Math.ceil(ratio * totalDocCounts);

        // Add clusters until we've covered enough documents
        int totalDocsExamined = 0;
        int numClustersNeeded = 0;

        while (totalDocsExamined < documentsToExamine && numClustersNeeded < dotProductWithClusterRepresentatives.length) {
            int clusterIndex = indices[numClustersNeeded];
            totalDocsExamined += clusterDocCounts[clusterIndex];
            numClustersNeeded++;
        }

        // Return result array with the top cluster IDs
        return Arrays.copyOfRange(indices, 0, numClustersNeeded);
    }

    public int getTopCluster(float[] querySketch, String sketchType) {
        float[] dotProductWithClusterRepresentatives = computeDotProductWithClusterRepresentatives(querySketch, sketchType);
        // Find the index of the maximum dot product
        int maxIndex = 0;
        float maxDotProduct = dotProductWithClusterRepresentatives[0];

        for (int i = 1; i < dotProductWithClusterRepresentatives.length; i++) {
            if (dotProductWithClusterRepresentatives[i] > maxDotProduct) {
                maxDotProduct = dotProductWithClusterRepresentatives[i];
                maxIndex = i;
            }
        }

        return maxIndex;
    }

    public int addDoc(float[] querySketch, String sketchType) {
        totalDocCounts += 1;
        int clusterId = getTopCluster(querySketch, sketchType);
        clusterDocCounts[clusterId] += 1;
        return clusterId;
    }
}

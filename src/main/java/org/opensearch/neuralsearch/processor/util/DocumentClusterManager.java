/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.security.AccessController;
import java.nio.file.Paths;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;

public class DocumentClusterManager {

    int totalDocCounts; // total number of documents within the index
    int[] clusterDocCounts; // number of documents across each cluster
    float[][] clusterRepresentatives; // an array of sketch vectors indicating the center of each cluster

    String clusterAssignmentFilePath = "/Users/yuyezhu/Desktop/Code/neural-search/build/resources/test/assignment.bin";
    String clusterRepresentativeFilePath = "/Users/yuyezhu/Desktop/Code/neural-search/build/resources/test/representatives.bin";

    // Instance is created at class loading time
    private static final DocumentClusterManager INSTANCE = new DocumentClusterManager();

    private DocumentClusterManager() {
        // Private constructor due to singleton
        loadClusterAssignment();
        loadClusterRepresentative();
    }

    public static DocumentClusterManager getInstance() {
        return INSTANCE;
    }

    public static float dotProduct(float[] sketch_1, float[] sketch_2) {
        // assume that sketch_1 and sketch_2 share the same length
        float sum = 0;
        for (int i = 0; i < sketch_1.length; i++) {
            sum += sketch_1[i] * sketch_2[i];
        }
        return sum;
    }

    private void loadClusterAssignment() {
        try {
            // Use AccessController to perform privileged file operations
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                byte[] assignmentBytes = Files.readAllBytes(Paths.get(clusterAssignmentFilePath));
                ByteBuffer assignmentBuffer = ByteBuffer.wrap(assignmentBytes).order(ByteOrder.nativeOrder());
                clusterDocCounts = new int[assignmentBytes.length / 4];
                for (int i = 0; i < clusterDocCounts.length; i++) {
                    clusterDocCounts[i] = assignmentBuffer.getInt(i * 4);
                    totalDocCounts += clusterDocCounts[i];
                }
                return null;
            });

            System.out.println("Loaded " + clusterDocCounts.length + " cluster assignments");
            System.out.println("Total document count: " + totalDocCounts);

        } catch (PrivilegedActionException e) {
            System.err.println("Error during privileged file access " + e.getException());
        }
    }

    public void loadClusterRepresentative() {
        try {
            // Use AccessController to perform privileged file operations
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                int rows = 11896;
                int cols = 1024;
                byte[] representativeBytes = Files.readAllBytes(Paths.get(clusterRepresentativeFilePath));
                ByteBuffer representativeBuffer = ByteBuffer.wrap(representativeBytes).order(ByteOrder.nativeOrder());
                clusterRepresentatives = new float[rows][cols];
                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        clusterRepresentatives[i][j] = representativeBuffer.getFloat((i * cols + j) * 4);
                    }
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
    }

    private float[] getDotProductWithClusterRepresentatives(float[] querySketch) {
        float[] dotProductWithClusterRepresentatives = new float[clusterRepresentatives.length];
        for (int i = 0; i < clusterRepresentatives.length; i += 1) {
            dotProductWithClusterRepresentatives[i] = dotProduct(querySketch, clusterRepresentatives[i]);
        }
        return dotProductWithClusterRepresentatives;
    }

    public int[] getTopClusters(float[] querySketch, float ratio) throws IllegalArgumentException {
        if (ratio > 1 || ratio <= 0) {
            throw new IllegalArgumentException("ratio should be in (0, 1]");
        }
        float[] dotProductWithClusterRepresentatives = getDotProductWithClusterRepresentatives(querySketch);

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
        return Arrays.stream(indices).limit(numClustersNeeded).mapToInt(Integer::intValue).toArray();
    }

    public int getTopCluster(float[] querySketch) throws IllegalStateException {
        float[] dotProductWithClusterRepresentatives = getDotProductWithClusterRepresentatives(querySketch);
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

    public void addDoc(float[] querySketch) {
        totalDocCounts += 1;
        clusterDocCounts[getTopCluster(querySketch)] += 1;
    }
}

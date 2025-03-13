/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.util.Arrays;

public class DocumentClusterManager {

    int totalDocCounts; // total number of documents within the index
    int[] clusterDocCounts; // number of documents across each cluster
    float[][] clusterRepresentatives; // an array of sketch vectors indicating the center of each cluster
    String clusterRepresentativeFilePath = "";
    
    // Instance is created at class loading time
    private static final DocumentClusterManager INSTANCE = new DocumentClusterManager();

    private DocumentClusterManager() {
        // Private constructor due to singleton
        load(clusterRepresentativeFilePath);
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

    public void load(String clusterRepresentativeFilePath) {
        // read cluster_representatives from a file
        // please add some random values to clusterDocCounts and clusterRepresentatives
        // for testing purpose
        totalDocCounts = 1000;
        clusterDocCounts = new int[10];
        clusterRepresentatives = new float[10][10];
        for (int i = 0; i < clusterDocCounts.length; i++) {
            clusterDocCounts[i] = 100;
            for (int j = 0; j < clusterRepresentatives[i].length; j++) {
                clusterRepresentatives[i][j] = (float) Math.random();
            }
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

        // Create result array with the top cluster IDs
        int[] topClusters = new int[numClustersNeeded];
        System.arraycopy(indices, 0, topClusters, 0, numClustersNeeded);

        return topClusters;
    }

    public int findTopCluster(float[] querySketch) throws IllegalStateException {
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
        clusterDocCounts[findTopCluster(querySketch)] += 1;
    }
}

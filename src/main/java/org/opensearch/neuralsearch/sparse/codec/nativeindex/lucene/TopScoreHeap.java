/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

/**
 * Bounded top-k of (score, id) pairs, a transcription of nsparse's {@code TopKHolder}.
 *
 * <p>A binary min-heap of at most k entries, so the smallest kept score is at the root and is the
 * admission threshold. The rejection is {@code score <= threshold}, not {@code <}, which matters:
 * a tie with the k-th score does not displace it, so which of several equal scores survives is
 * decided by visit order -- the same "arbitrary among equals" the C++ heap gives, though not
 * necessarily the same arbitrary one, since the two visit blocks in different orders.
 *
 * <p>Also used for the {@code cut} tokens and the {@code k'} block budget, which are the same
 * "keep the k largest" problem over different payloads.
 */
final class TopScoreHeap {

    private final float[] scores;
    private final int[] ids;
    private final int capacity;
    private int size;

    TopScoreHeap(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("top-k capacity must be positive, got " + capacity);
        }
        this.capacity = capacity;
        // One-based, so a node's children are 2i and 2i + 1 without an offset at every step.
        this.scores = new float[capacity + 1];
        this.ids = new int[capacity + 1];
    }

    void clear() {
        size = 0;
    }

    void add(float score, int id) {
        if (size == capacity) {
            if (score <= scores[1]) {
                return;
            }
            scores[1] = score;
            ids[1] = id;
            siftDown();
            return;
        }
        int node = ++size;
        scores[node] = score;
        ids[node] = id;
        siftUp(node);
    }

    int size() {
        return size;
    }

    /** The smallest score kept, i.e. the current admission threshold. Only valid when non-empty. */
    float threshold() {
        return scores[1];
    }

    /**
     * Empties the heap into {@code outScores}/{@code outIds} in descending score order, and returns
     * how many entries it wrote. Destructive, like the C++ {@code top_k_items_descending}.
     */
    int drainDescending(float[] outScores, int[] outIds) {
        int count = size;
        for (int i = count - 1; i >= 0; --i) {
            outScores[i] = scores[1];
            outIds[i] = ids[1];
            scores[1] = scores[size];
            ids[1] = ids[size];
            --size;
            siftDown();
        }
        return count;
    }

    private void siftUp(int node) {
        while (node > 1) {
            int parent = node >>> 1;
            if (scores[parent] <= scores[node]) {
                return;
            }
            swap(parent, node);
            node = parent;
        }
    }

    private void siftDown() {
        int node = 1;
        while (true) {
            int child = node << 1;
            if (child > size) {
                return;
            }
            if (child + 1 <= size && scores[child + 1] < scores[child]) {
                ++child;
            }
            if (scores[node] <= scores[child]) {
                return;
            }
            swap(node, child);
            node = child;
        }
    }

    private void swap(int a, int b) {
        float score = scores[a];
        scores[a] = scores[b];
        scores[b] = score;
        int id = ids[a];
        ids[a] = ids[b];
        ids[b] = id;
    }
}

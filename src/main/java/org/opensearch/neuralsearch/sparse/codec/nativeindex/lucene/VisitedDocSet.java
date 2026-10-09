/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import java.util.Arrays;

/**
 * The set of documents a query has already scored, reused across queries on one searcher.
 *
 * <p>A document's vector is copied into every block it belongs to, so the blocks a query selects
 * overlap and the same document is reached several times; without dedup the top-k heap would fill
 * with copies of one document and drop others. This is the {@code absl::flat_hash_set<idx_t>} the
 * C++ search keeps per thread, with the same lifetime -- cleared per query, not reallocated.
 *
 * <p>Open addressing with a per-query generation stamp, so {@link #clear()} is a counter increment
 * rather than a wipe of the table.
 */
final class VisitedDocSet {

    /** Grow at three quarters full: past that, probe lengths on a linear-probing table climb fast. */
    private static final float LOAD_FACTOR = 0.75f;
    /** Knuth's multiplicative constant; doc ids are dense, which a low-bit mask alone spreads badly. */
    private static final int HASH_MIXER = 0x9E37_79B1;

    private int[] keys;
    private int[] stamps;
    private int mask;
    private int threshold;
    private int generation;
    private int size;

    VisitedDocSet(int expectedSize) {
        allocate(tableSizeFor(expectedSize));
    }

    /** Forgets every member. The table is kept, so the next query pays no allocation. */
    void clear() {
        size = 0;
        if (++generation == 0) {
            // Wrapped: a stale stamp of 0 would read as a live entry. Costs a wipe once per 2^32
            // queries on one searcher.
            Arrays.fill(stamps, 0);
            generation = 1;
        }
    }

    /**
     * Adds {@code key}.
     *
     * @return true if it was not already present, i.e. the caller should score this document
     */
    boolean add(int key) {
        int slot = slotOf(key);
        if (stamps[slot] == generation) {
            return false;
        }
        keys[slot] = key;
        stamps[slot] = generation;
        if (++size >= threshold) {
            grow();
        }
        return true;
    }

    private int slotOf(int key) {
        int slot = (key * HASH_MIXER) & mask;
        while (stamps[slot] == generation && keys[slot] != key) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    private void grow() {
        int[] oldKeys = keys;
        int[] oldStamps = stamps;
        int oldGeneration = generation;
        allocate(keys.length << 1);
        generation = oldGeneration;
        size = 0;
        for (int i = 0; i < oldKeys.length; ++i) {
            if (oldStamps[i] == oldGeneration) {
                int slot = slotOf(oldKeys[i]);
                keys[slot] = oldKeys[i];
                stamps[slot] = generation;
                ++size;
            }
        }
    }

    private void allocate(int capacity) {
        keys = new int[capacity];
        stamps = new int[capacity];
        mask = capacity - 1;
        threshold = (int) (capacity * LOAD_FACTOR);
        generation = 1;
    }

    /** Smallest power of two that holds {@code expectedSize} entries under the load factor. */
    private static int tableSizeFor(int expectedSize) {
        int needed = Math.max(16, (int) Math.min(1L << 30, (long) (expectedSize / LOAD_FACTOR) + 1));
        return Integer.highestOneBit(needed - 1) << 1;
    }
}

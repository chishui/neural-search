/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.opensearch.test.OpenSearchTestCase;

public class TopScoreHeapTests extends OpenSearchTestCase {

    public void testKeepsTheHighestScoresInDescendingOrder() {
        TopScoreHeap heap = new TopScoreHeap(3);
        heap.add(1.0f, 10);
        heap.add(5.0f, 50);
        heap.add(3.0f, 30);
        heap.add(4.0f, 40);
        heap.add(0.5f, 5);

        float[] scores = new float[3];
        int[] ids = new int[3];
        assertEquals(3, heap.drainDescending(scores, ids));
        assertArrayEquals(new float[] { 5.0f, 4.0f, 3.0f }, scores, 0.0f);
        assertArrayEquals(new int[] { 50, 40, 30 }, ids);
    }

    public void testDrainsFewerThanCapacityWhenUnderfilled() {
        TopScoreHeap heap = new TopScoreHeap(5);
        heap.add(2.0f, 20);
        heap.add(1.0f, 10);

        float[] scores = new float[5];
        int[] ids = new int[5];
        assertEquals(2, heap.drainDescending(scores, ids));
        assertEquals(2.0f, scores[0], 0.0f);
        assertEquals(1.0f, scores[1], 0.0f);
    }

    /**
     * The threshold rejection is {@code <=}, matching the C++ heap: a tie with the k-th score does not
     * displace it, so the earlier entry survives.
     */
    public void testATieWithTheThresholdDoesNotDisplaceIt() {
        TopScoreHeap heap = new TopScoreHeap(2);
        heap.add(1.0f, 1);
        heap.add(2.0f, 2);
        heap.add(1.0f, 99);

        float[] scores = new float[2];
        int[] ids = new int[2];
        heap.drainDescending(scores, ids);
        assertArrayEquals(new int[] { 2, 1 }, ids);
    }

    public void testThresholdIsTheSmallestKeptScore() {
        TopScoreHeap heap = new TopScoreHeap(2);
        heap.add(4.0f, 4);
        heap.add(2.0f, 2);

        assertEquals(2.0f, heap.threshold(), 0.0f);
    }

    public void testClearForgetsEverything() {
        TopScoreHeap heap = new TopScoreHeap(2);
        heap.add(4.0f, 4);
        heap.clear();

        assertEquals(0, heap.size());
    }

    public void testRejectsANonPositiveCapacity() {
        expectThrows(IllegalArgumentException.class, () -> new TopScoreHeap(0));
    }
}

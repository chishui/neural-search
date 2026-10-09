/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class VisitedDocSetTests extends OpenSearchTestCase {

    public void testFirstAddSucceedsAndRepeatsDoNot() {
        VisitedDocSet visited = new VisitedDocSet(16);

        assertTrue(visited.add(7));
        assertFalse(visited.add(7));
        assertTrue(visited.add(0));
        assertFalse(visited.add(0));
    }

    /** Zero is a real doc id, so emptiness cannot be represented by a zero key. */
    public void testZeroIsAMemberLikeAnyOther() {
        VisitedDocSet visited = new VisitedDocSet(16);
        visited.add(0);

        assertFalse(visited.add(0));
        assertTrue(visited.add(1));
    }

    public void testClearForgetsEverythingWithoutLosingCapacity() {
        VisitedDocSet visited = new VisitedDocSet(16);
        visited.add(3);
        visited.clear();

        assertTrue("a cleared set has no members", visited.add(3));
    }

    /** Growth has to carry the current query's members over, or a doc could be scored twice. */
    public void testGrowsBeyondItsInitialCapacity() {
        VisitedDocSet visited = new VisitedDocSet(16);
        Set<Integer> expected = new HashSet<>();
        Random random = new Random(42);
        for (int i = 0; i < 5000; ++i) {
            int key = random.nextInt(1000);
            assertEquals("add must agree with a reference set for key " + key, expected.add(key), visited.add(key));
        }
    }

    public void testHandlesNegativeAndExtremeKeys() {
        VisitedDocSet visited = new VisitedDocSet(16);

        assertTrue(visited.add(Integer.MIN_VALUE));
        assertTrue(visited.add(Integer.MAX_VALUE));
        assertFalse(visited.add(Integer.MIN_VALUE));
        assertFalse(visited.add(Integer.MAX_VALUE));
    }
}

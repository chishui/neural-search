/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.List;

public class SortedDocInClusterIterator extends DocIdSetIterator {
    private final List<Integer> sortedList;
    private int current;

    public SortedDocInClusterIterator(List<Integer> docs) {
        this.sortedList = docs;
        this.sortedList.sort(Integer::compareTo);
        this.current = -1;
    }

    @Override
    public int docID() {
        if (current >= 0 && current < sortedList.size()) {
            return sortedList.get(current);
        }
        return -1;  // or NO_MORE_DOCS if at end
    }

    @Override
    public int nextDoc() throws IOException {
        if (current + 1 < sortedList.size()) {
            current++;
            return docID();
        }
        return NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
        while (nextDoc() != NO_MORE_DOCS) {
            if (docID() >= target) {
                return docID();
            }
        }
        return NO_MORE_DOCS;
    }

    @Override
    public long cost() {
        return 0;
    }

    public void reset() {
        current = -1;
    }
}

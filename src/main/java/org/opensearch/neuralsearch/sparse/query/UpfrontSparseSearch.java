/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * The top-k fetch shared by the scorers that get their whole result set up front rather than from a
 * cursor.
 *
 * <p>Both the native engine and the {@code IndexInput} reader return a finished top-k from one call,
 * so k is decided before iteration and a collector cannot make the query stop early. What that
 * requires of the caller is the same either way, and is all here: a query filter goes down as a
 * candidate set for the engine to restrict itself to, deletions do not and are dropped from the
 * results afterwards, and a fetch left short by those deletions is retried larger.
 */
final class UpfrontSparseSearch {

    private UpfrontSparseSearch() {}

    /** One engine's top-k call: the tokens and weights are the caller's, already captured. */
    @FunctionalInterface
    interface Engine {
        /**
         * @param fetchSize    how many hits to ask for
         * @param filterDocIds the candidate set to restrict the search to, or null for none
         * @return the hits, highest score first, with no padding entries
         */
        SparseQueryResult[] search(int fetchSize, long[] filterDocIds) throws IOException;
    }

    /**
     * Runs the fetch and returns the hits in ascending doc-id order.
     *
     * @param resultSize          hits the query asked for
     * @param maxDoc              the segment's doc count, which caps how large a fetch can usefully get
     * @param acceptedDocs        live docs, or null when nothing is deleted
     * @param filterBitSetIterator the query filter's docs, or null when the query has no filter
     * @param engine              the engine to fetch from
     */
    static List<Pair<Integer, Float>> execute(
        int resultSize,
        int maxDoc,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        Engine engine
    ) throws IOException {
        // Only a query filter is a candidate set. Live docs are a mask: handing them down as a filter
        // would make every query on a segment with a single deletion an enumeration of every live doc,
        // and once the live count drops to <= k the engine switches to an exact match that returns all
        // of them -- including docs sharing no token with the query, at score 0. Lucene skips deleted
        // docs while traversing postings instead, so drop them from the results below.
        final long[] docIds = filterBitSetIterator == null ? null : constructFilterList(acceptedDocs, filterBitSetIterator);
        final boolean maskDeletedDocs = docIds == null && acceptedDocs != null;

        // Deleted hits are dropped after the fact, so a fetch of exactly k can come back short. Grow
        // and retry rather than budget for every deletion upfront: numDeletedDocs() is segment-wide, so
        // k + numDeletedDocs() turns a k=10 query on a segment with 10K deletions into a top-10010 --
        // and in seismic a larger k also raises the heap's admission threshold more slowly, so it visits
        // far more blocks. Deletions rarely reach the top k, so the common case pays exactly k and the
        // loop runs once.
        int fetchSize = maskDeletedDocs ? Math.min(resultSize, maxDoc) : resultSize;
        List<Pair<Integer, Float>> hits;
        while (true) {
            SparseQueryResult[] results = engine.search(fetchSize, docIds);
            Stream<Pair<Integer, Float>> stream = Arrays.stream(results).map(r -> Pair.of(r.getId(), r.getScore()));
            if (maskDeletedDocs) {
                stream = stream.filter(hit -> acceptedDocs.get(hit.getLeft())).limit(resultSize);
            }
            hits = stream.toList();
            // A short result set means the engine had nothing more to give at this fetch size, so a
            // bigger one cannot fill the gap either.
            if (maskDeletedDocs == false || hits.size() >= resultSize || results.length < fetchSize || fetchSize >= maxDoc) {
                break;
            }
            fetchSize = nextFetchSize(fetchSize, hits.size(), resultSize, maxDoc);
        }
        // The engine returns hits in score order, but a DocIdSetIterator must walk doc IDs in ascending
        // order. Emitting them by score trips a Lucene assertion as soon as the scorer is wrapped in a
        // conjunction -- a nested query does that, and the assertion kills the node. The score order has
        // to survive until after the limit above, which keeps the k best rather than k arbitrary.
        return hits.stream().sorted(Comparator.comparingInt(Pair::getLeft)).toList();
    }

    /**
     * Grows the fetch to at least what the surviving fraction implies is needed, so a segment whose top
     * hits are mostly deleted converges in a call or two instead of doubling from k up to maxDoc.
     * Doubling is the floor, which keeps the growth geometric when nothing survived to extrapolate from.
     */
    private static int nextFetchSize(int fetchSize, int survivors, int resultSize, int maxFetchSize) {
        long doubled = (long) fetchSize * 2;
        long projected = survivors == 0 ? doubled : (long) Math.ceil((double) fetchSize * resultSize / survivors);
        return (int) Math.min(maxFetchSize, Math.max(doubled, projected));
    }

    private static long[] constructFilterList(Bits acceptedDocs, BitSetIterator filterBitSetIterator) throws IOException {
        // SparseVectorQuery.createBitSet already intersects the filter with live docs; this is belt and
        // braces for a filter bitset that reached us without that step.
        DocIdSetIterator iterator = (acceptedDocs != null)
            ? new FilteredBitSetIterator(filterBitSetIterator, acceptedDocs)
            : filterBitSetIterator;

        // cost() is the filter bitset's cardinality, so it is exact unless the live-doc mask above drops
        // something -- an upper bound either way, and the array is trimmed to fit at the end.
        long[] docIds = new long[Math.max(1, (int) Math.min(iterator.cost(), Integer.MAX_VALUE))];
        int size = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docIds = ArrayUtil.grow(docIds, size + 1);
            docIds[size++] = doc;
        }
        return size == docIds.length ? docIds : ArrayUtil.copyOfSubArray(docIds, 0, size);
    }

    /** Iterates a BitSetIterator while also checking acceptedDocs (liveDocs). */
    private static class FilteredBitSetIterator extends DocIdSetIterator {
        private final BitSetIterator delegate;
        private final Bits acceptedDocs;

        FilteredBitSetIterator(BitSetIterator delegate, Bits acceptedDocs) {
            this.delegate = delegate;
            this.acceptedDocs = acceptedDocs;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc;
            while ((doc = delegate.nextDoc()) != NO_MORE_DOCS) {
                if (acceptedDocs.get(doc)) {
                    return doc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
            if (acceptedDocs.get(doc)) return doc;
            return nextDoc();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }
}

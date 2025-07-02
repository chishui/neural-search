/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.algorithm.SeismicBaseScorer;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.List;

public class OrderedPostingWithClustersScorer extends SeismicBaseScorer {

    private final Similarity.SimScorer simScorer;
    private final DocIdSetIterator conjunctionDisi;

    public OrderedPostingWithClustersScorer(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReader leafReader,
        Bits acceptedDocs,
        SparseVectorReader reader,
        Similarity.SimScorer simScorer,
        BitSetIterator filterBitSetIterator
    ) throws IOException {
        super(leafReader, fieldName, sparseQueryContext, leafReader.maxDoc(), queryVector, reader, acceptedDocs);
        this.simScorer = simScorer;
        List<Pair<Integer, Integer>> results = searchUpfront(sparseQueryContext.getK());
        ResultsDocValueIterator resultsIterator = new ResultsDocValueIterator(results);
        if (filterBitSetIterator != null) {
            conjunctionDisi = ConjunctionUtils.intersectIterators(List.of(resultsIterator, filterBitSetIterator));
        } else {
            conjunctionDisi = resultsIterator;
        }
    }

    @Override
    public int docID() {
        return conjunctionDisi.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return conjunctionDisi;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        return this.simScorer.score(conjunctionDisi.cost(), 0);
    }

    static class ResultsDocValueIterator extends DocIdSetIterator {
        private final IteratorWrapper<Pair<Integer, Integer>> resultsIterator;
        private int docId;

        ResultsDocValueIterator(List<Pair<Integer, Integer>> results) {
            resultsIterator = new IteratorWrapper<>(results.iterator());
            docId = -1;
        }

        @Override
        public int docID() {
            return docId;
        }

        @Override
        public int nextDoc() throws IOException {
            if (resultsIterator.next() == null) {
                docId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            docId = resultsIterator.getCurrent().getLeft();
            return docId;
        }

        @Override
        public int advance(int target) throws IOException {
            if (target <= docId) {
                return docId;
            }
            while (resultsIterator.hasNext()) {
                Pair<Integer, Integer> pair = resultsIterator.next();
                if (pair.getKey() >= target) {
                    docId = pair.getKey();
                    return docId;
                }
            }
            docId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }

        // we use cost() to return prestored score
        @Override
        public long cost() {
            if (resultsIterator.getCurrent() == null || !resultsIterator.hasNext()) {
                return 0;
            } else {
                return resultsIterator.getCurrent().getValue();
            }
        }
    }

}

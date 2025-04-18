/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class SparseQueryWeight extends Weight {

    public SparseQueryWeight(SparseVectorQuery query) {
        super(query);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return null;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final int maxDoc = context.reader().maxDoc();
        final SparseVectorQuery query = (SparseVectorQuery) parentQuery;
        final Scorer scorer = new PostingWithClustersScorer(
            query.getFieldName(),
            query.getTokens(),
            query.getQueryVector(),
            context,
            context.reader().getLiveDocs()
        );
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                return scorer;
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                return new BulkScorer() {
                    // We ignore the max value as our algorithm can't limit the docId to range of (min, max)
                    // so, to ensure it's only called once, we return the maxDoc
                    @Override
                    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
                        collector.setScorer(scorer);
                        DocIdSetIterator iter = scorer.iterator();
                        int docId = iter.nextDoc();
                        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            collector.collect(docId);
                            docId = iter.nextDoc();
                        }
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return 0;
                    }
                };
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return false;
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.query.SparseQueryContext;

import java.io.IOException;
import java.util.List;

@Log4j2
public class SeismicSearch extends SeismicSearchBase {
    // TODO: this can be configured in setting
    private static final int MAX_SEARCH_RESULT_SIZE = 10000;

    public SeismicSearch(
        String fieldName,
        SparseQueryContext sparseQueryContext,
        SparseVector queryVector,
        LeafReader leafReader,
        Bits acceptedDocs,
        SparseVectorReader reader
    ) throws IOException {
        super(leafReader, fieldName, sparseQueryContext, leafReader.maxDoc(), queryVector, reader, acceptedDocs);
        initialize(leafReader);
    }

    private void initialize(LeafReader leafReader) throws IOException {
        Terms terms = Terms.getTerms(leafReader, fieldName);
        assert terms != null : "Terms must not be null";

        for (String token : sparseQueryContext.getTokens()) {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term = new BytesRef(token);
            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            if (!(postingsEnum instanceof SparsePostingsEnum sparsePostingsEnum)) {
                log.error("posting enum is not SparsePostingsEnum, actual type: {}", postingsEnum.getClass().getName());
                return;
            }
            subScorers.add(new SingleScorer(sparsePostingsEnum, term));
        }
    }

    public List<Pair<Integer, Float>> search() throws IOException {
        HeapWrapper resultHeap = new HeapWrapper(MAX_SEARCH_RESULT_SIZE);
        for (Scorer scorer : subScorers) {
            DocIdSetIterator iterator = scorer.iterator();
            int docId = 0;
            while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (acceptedDocs != null && !acceptedDocs.get(docId)) {
                    continue;
                }
                if (visitedDocId.get(docId)) {
                    continue;
                }
                visitedDocId.set(docId);
                SparseVector doc = reader.read(docId);
                if (doc == null) {
                    continue;
                }
                float score = doc.dotProduct(queryDenseVector);
                scoreHeap.add(Pair.of(docId, score));
                resultHeap.add(Pair.of(docId, score));
                docId = iterator.nextDoc();
            }
        }
        return resultHeap.toOrderedList();
    }
}

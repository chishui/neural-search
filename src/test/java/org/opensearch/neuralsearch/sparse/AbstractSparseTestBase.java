/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractSparseTestBase extends OpenSearchQueryTestCase {

    protected DocFreqIterator constructDocFreqIterator(Integer... docs) {
        return constructDocFreqIterator(Arrays.asList(docs), Arrays.asList(docs));
    }

    protected DocFreqIterator constructDocFreqIterator(List<Integer> docs, List<Integer> freqs) {
        return new DocFreqIterator() {
            int i = -1;

            @Override
            public byte freq() {
                return (byte) (freqs.get(i) & 0xff);
            }

            @Override
            public int nextDoc() {
                if (i + 1 == docs.size()) {
                    return NO_MORE_DOCS;
                } else {
                    return docs.get(++i);
                }
            }

            @Override
            public int docID() {
                return i < 0 ? -1 : i == docs.size() ? NO_MORE_DOCS : docs.get(i);
            }

            @Override
            public long cost() {
                return docs.size();
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }
        };
    }

    protected List<DocFreq> preparePostings(int... docFreqs) {
        List<DocFreq> postings = new ArrayList<>();
        for (int i = 0; i < docFreqs.length; i += 2) {
            postings.add(new DocFreq(docFreqs[i], (byte) docFreqs[i + 1]));
        }
        return postings;
    }

    protected SparseVector createVector(int... docFreqs) {
        List<SparseVector.Item> items = new ArrayList<>();
        for (int i = 0; i < docFreqs.length; i += 2) {
            items.add(new SparseVector.Item(docFreqs[i], (byte) docFreqs[i + 1]));
        }
        return new SparseVector(items);
    }

    protected void preparePostings(LeafReader leafReader, String fieldName, Terms terms, TermsEnum termsEnum,
        SparsePostingsEnum postingsEnum, Map<String, Boolean> seekExact)
        throws IOException {
        when(leafReader.terms(eq(fieldName))).thenReturn(terms);
        when(terms.iterator()).thenReturn(termsEnum);

        // Setup termsEnum to return true for first token and false for second token
        for (Map.Entry<String, Boolean> entry : seekExact.entrySet()) {
            when(termsEnum.seekExact(eq(new BytesRef(entry.getKey())))).thenReturn(entry.getValue());
        }

        // Setup postingsEnum
        when(termsEnum.postings(null, PostingsEnum.FREQS)).thenReturn(postingsEnum);
    }

    protected void prepareCluster(SparsePostingsEnum postingsEnum) {
        // Setup cluster iterator for postingsEnum
        DocumentCluster cluster = mock(DocumentCluster.class);
        DocFreqIterator docFreqIterator = constructDocFreqIterator(1, 2, 3);
        when(cluster.getDisi()).thenReturn(docFreqIterator);
        when(cluster.isShouldNotSkip()).thenReturn(true);

        SparseVector summaryVector = createVector(1, 2, 2, 3);
        when(cluster.getSummary()).thenReturn(summaryVector);

        IteratorWrapper<DocumentCluster> clusterIterator = mock(IteratorWrapper.class);
        when(clusterIterator.next()).thenReturn(cluster).thenReturn(null);
        when(postingsEnum.clusterIterator()).thenReturn(clusterIterator);
    }
}

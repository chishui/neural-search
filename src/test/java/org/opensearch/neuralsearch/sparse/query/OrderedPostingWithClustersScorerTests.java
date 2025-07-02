/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class OrderedPostingWithClustersScorerTests extends AbstractSparseTestBase {

    @Mock
    private LeafReader leafReader;

    @Mock
    private SparseQueryContext sparseQueryContext;

    @Mock
    private SparseVectorReader vectorReader;

    @Mock
    private Terms terms;

    @Mock
    private TermsEnum termsEnum;

    @Mock
    private SparsePostingsEnum postingsEnum;

    private Similarity.SimScorer simScorer;

    private static final String FIELD_NAME = "test_field";
    private static final int MAX_DOC_COUNT = 10;
    private static final float score = 34.0f;
    private static final int K_VALUE = 5;
    private static final List<String> TEST_TOKENS = Arrays.asList("token1", "token2");

    private OrderedPostingWithClustersScorer scorer;
    private SparseVector queryVector;
    private List<Pair<Integer, Integer>> searchResults;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Initialize mocks
        MockitoAnnotations.openMocks(this);

        // Setup query vector
        queryVector = createVector(1, 5, 2, 3, 3, 7);
        searchResults = Arrays.asList(Pair.of(3, 1), Pair.of(2, 2), Pair.of(3, 3));
        // Setup sparse query context
        when(sparseQueryContext.getTokens()).thenReturn(TEST_TOKENS);
        when(sparseQueryContext.getK()).thenReturn(K_VALUE);
        when(sparseQueryContext.getHeapFactor()).thenReturn(2.0f);

        // Setup leaf reader
        when(leafReader.maxDoc()).thenReturn(MAX_DOC_COUNT);
        preparePostings(leafReader, FIELD_NAME, terms, termsEnum, postingsEnum, Map.of("token1", true, "token2", false));
        prepareCluster(postingsEnum);
        // Setup vector reader
        SparseVector docVector = createVector(1, 5, 2, 3);
        when(vectorReader.read(anyInt())).thenReturn(docVector);

        // Setup simScorer
        simScorer = new Similarity.SimScorer() {
            @Override
            public float score(float freq, long norm) {
                return freq;
            }
        };
    }

    public void testConstructorWithoutFilter() throws IOException {
        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                null
            )
        );

        // Mock searchUpfront to return our predefined results
        doReturn(searchResults).when(scorerSpy).searchUpfront(eq(K_VALUE));

        // Test iterator
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(2, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(3, iterator.nextDoc());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testConstructorWithFilter() throws IOException {
        // Create a BitSetIterator for filtering
        FixedBitSet bitSet = new FixedBitSet(MAX_DOC_COUNT);
        bitSet.set(1);
        bitSet.set(5);
        BitSetIterator filterBitSetIterator = new BitSetIterator(bitSet, 2);

        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                filterBitSetIterator
            )
        );

        // Test iterator - should only return doc 1 and 5 (intersection of results and filter)
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(1, iterator.nextDoc());
        assertEquals(NO_MORE_DOCS, iterator.nextDoc());
    }

    public void testDocID() throws IOException {
        // Create a spy of OrderedPostingWithClustersScorer to mock searchUpfront
        OrderedPostingWithClustersScorer scorerSpy = spy(
            new OrderedPostingWithClustersScorer(
                FIELD_NAME,
                sparseQueryContext,
                queryVector,
                leafReader,
                null,
                vectorReader,
                simScorer,
                null
            )
        );

        // Mock searchUpfront to return our predefined results
        doReturn(searchResults).when(scorerSpy).searchUpfront(eq(K_VALUE));

        // Test docID
        DocIdSetIterator iterator = scorerSpy.iterator();
        assertEquals(-1, scorerSpy.docID());

        iterator.nextDoc();
        assertEquals(1, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(2, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(3, scorerSpy.docID());
        assertEquals(score, scorerSpy.score(), DELTA_FOR_ASSERTION);

        iterator.nextDoc();
        assertEquals(NO_MORE_DOCS, scorerSpy.docID());
    }
}

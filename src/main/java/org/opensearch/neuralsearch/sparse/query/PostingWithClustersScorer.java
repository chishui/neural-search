/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.codec.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class PostingWithClustersScorer extends Scorer {

    private final String fieldName;
    private final List<String> queryTokens;
    private final SparseVector queryVector;
    // The heap to maintain docId and its similarity score with query
    private final PriorityQueue<Pair<Integer, Float>> scoreHeap = new PriorityQueue<>((a, b) -> Float.compare(a.getRight(), b.getRight()));
    private final static int MAX_QUEUE_SIZE = 100;
    private final static float HEAP_FACTOR = 1.0f;
    private final LongBitSet visitedDocId;
    private SparseVectorForwardIndex.SparseVectorForwardIndexReader reader;
    private List<Scorer> subScorers = new ArrayList<>();
    private int subScorersIndex = 0;
    private Terms terms;
    private float score;
    private final Bits acceptedDocs;

    public PostingWithClustersScorer(
        String fieldName,
        List<String> queryTokens,
        SparseVector queryVector,
        LeafReaderContext context,
        Bits acceptedDocs
    ) throws IOException {
        this.queryTokens = queryTokens;
        this.fieldName = fieldName;
        this.queryVector = queryVector;
        this.visitedDocId = new LongBitSet(context.reader().maxDoc());
        this.acceptedDocs = acceptedDocs;
        initialize(context.reader());
    }

    private void initialize(LeafReader leafReader) throws IOException {
        terms = Terms.getTerms(leafReader, fieldName);
        BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldName);
        for (String token : queryTokens) {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term = new BytesRef(token);
            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postingsEnum = termsEnum.postings(null, PostingsEnum.FREQS);
            if (postingsEnum instanceof SparsePostingsEnum) {
                SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postingsEnum;
                if (null == reader) {
                    reader = InMemorySparseVectorForwardIndex.getOrCreate(sparsePostingsEnum.getIndexKey()).getForwardIndexReader();
                }
                subScorers.add(new SingleScorer(sparsePostingsEnum, term));
            }
        }
    }

    private boolean isHeapFull() {
        return scoreHeap.size() == MAX_QUEUE_SIZE;
    }

    private void addToHeap(Pair<Integer, Float> pair) {
        scoreHeap.add(pair);
        if (isHeapFull()) {
            scoreHeap.poll();
        }
    }

    @Override
    public int docID() {
        return iterator().docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return new DocIdSetIterator() {
            @Override
            public int docID() {
                if (subScorersIndex == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                if (subScorersIndex >= subScorers.size()) {
                    return NO_MORE_DOCS;
                }
                return subScorers.get(subScorersIndex).iterator().docID();
            }

            @Override
            public int nextDoc() throws IOException {
                if (subScorersIndex == NO_MORE_DOCS) {
                    return NO_MORE_DOCS;
                }
                if (subScorersIndex >= subScorers.size()) {
                    subScorersIndex = NO_MORE_DOCS;
                    return NO_MORE_DOCS;
                }
                Scorer scorer = subScorers.get(subScorersIndex);
                int docId = scorer.iterator().nextDoc();
                if (docId == NO_MORE_DOCS) {
                    subScorersIndex++;
                    return nextDoc();
                } else {
                    if (visitedDocId.get(docId)) {
                        return nextDoc();
                    }
                    if (acceptedDocs != null && !acceptedDocs.get(docId)) {
                        return nextDoc();
                    }
                    visitedDocId.set(docId);
                    SparseVector doc = reader.readSparseVector(docId);
                    score = doc.dotProduct(queryVector);
                    addToHeap(Pair.of(docId, score));
                    return docId;
                }
            }

            @Override
            public int advance(int target) throws IOException {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        return score;
    }

    class SingleScorer extends Scorer {
        private final SparsePostingsEnum postingsEnum;
        private final BytesRef term;

        public SingleScorer(SparsePostingsEnum postingsEnum, BytesRef term) throws IOException {
            this.postingsEnum = postingsEnum;
            this.term = term;
        }

        @Override
        public int docID() {
            return postingsEnum.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {
                @Override
                public int docID() {
                    return postingsEnum.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    int docId = postingsEnum.nextDoc();
                    if (docId == -1 || docId == DocIdSetIterator.NO_MORE_DOCS) {
                        DocumentCluster cluster = postingsEnum.nextCluster();
                        while (cluster != null) {
                            if (cluster.isShouldNotSkip()) {
                                break;
                            }
                            assert cluster.getSummary() != null;
                            float score = cluster.getSummary().dotProduct(queryVector);
                            if (scoreHeap.size() == MAX_QUEUE_SIZE && score < scoreHeap.peek().getRight() / HEAP_FACTOR) {
                                cluster = postingsEnum.nextCluster();
                                continue;
                            }
                            break;
                        }
                        if (cluster == null) {
                            return DocIdSetIterator.NO_MORE_DOCS;
                        }
                        return postingsEnum.nextDoc();
                    } else {
                        return docId;
                    }
                }

                @Override
                public int advance(int target) throws IOException {
                    return 0;
                }

                @Override
                public long cost() {
                    return 0;
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return 0;
        }

        @Override
        public float score() throws IOException {
            return 0;
        }
    }
}

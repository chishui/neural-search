/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsEnum;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.query.SparseQueryContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

public abstract class SeismicSearchBase {
    protected final HeapWrapper scoreHeap;
    protected float heapThreshold = Float.MIN_VALUE;
    protected final LongBitSet visitedDocId;
    protected final String fieldName;
    protected final SparseQueryContext sparseQueryContext;
    protected final byte[] queryDenseVector;
    protected final Bits acceptedDocs;
    protected SparseVectorReader reader;
    protected List<Scorer> subScorers = new ArrayList<>();

    public SeismicSearchBase(
        LeafReader leafReader,
        String fieldName,
        SparseQueryContext sparseQueryContext,
        int maxDocCount,
        SparseVector queryVector,
        SparseVectorReader reader,
        Bits acceptedDocs
    ) throws IOException {
        visitedDocId = new LongBitSet(maxDocCount);
        this.fieldName = fieldName;
        this.sparseQueryContext = sparseQueryContext;
        this.queryDenseVector = queryVector.toDenseVector();
        this.reader = reader;
        this.acceptedDocs = acceptedDocs;
        scoreHeap = new HeapWrapper(sparseQueryContext.getK());
        if (reader == null) {
            BinaryDocValues docValues = leafReader.getBinaryDocValues(fieldName);
            if (docValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValuesPassThrough) {
                this.reader = sparseBinaryDocValuesPassThrough;
            }
        }
    }

    protected static PriorityQueue<Pair<Integer, Float>> makeHeap() {
        return new PriorityQueue<>((a, b) -> Float.compare(a.getRight(), b.getRight()));
    }

    class HeapWrapper {
        private final PriorityQueue<Pair<Integer, Float>> heap = makeHeap();
        private float heapThreshold = Float.MIN_VALUE;
        private final int K;

        HeapWrapper(int K) {
            this.K = K;
        }

        public void add(Pair<Integer, Float> pair) {
            if (pair.getRight() > heapThreshold) {
                heap.add(pair);
                if (heap.size() > K) {
                    heap.poll();
                    heapThreshold = heap.peek().getRight();
                }
            }
        }

        public List<Pair<Integer, Float>> toList() {
            return new ArrayList<>(heap);
        }

        public List<Pair<Integer, Float>> toOrderedList() {
            List<Pair<Integer, Float>> list = new ArrayList<>(heap);
            list.sort((a, b) -> Float.compare(a.getLeft(), b.getLeft()));
            return list;
        }

        public int size() {
            return heap.size();
        }

        public Pair<Integer, Float> peek() {
            return heap.peek();
        }
    }

    class SingleScorer extends Scorer {
        private final IteratorWrapper<DocumentCluster> clusterIter;
        private DocFreqIterator docs = null;

        public SingleScorer(SparsePostingsEnum postingsEnum, BytesRef term) throws IOException {
            clusterIter = postingsEnum.clusterIterator();
        }

        @Override
        public int docID() {
            if (docs == null) {
                return -1;
            }
            return docs.docID();
        }

        @Override
        public DocIdSetIterator iterator() {
            return new DocIdSetIterator() {

                private DocumentCluster nextQualifiedCluster() {
                    DocumentCluster cluster = clusterIter.next();
                    while (cluster != null) {
                        if (cluster.isShouldNotSkip()) {
                            return cluster;
                        }
                        int score = cluster.getSummary().dotProduct(queryDenseVector);
                        if (scoreHeap.size() == sparseQueryContext.getK()
                            && score < Objects.requireNonNull(scoreHeap.peek()).getRight() / sparseQueryContext.getHeapFactor()) {
                            cluster = clusterIter.next();
                        } else {
                            return cluster;
                        }
                    }
                    return null;
                }

                @Override
                public int docID() {
                    if (docs == null) {
                        return -1;
                    }
                    return docs.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    DocumentCluster cluster = null;
                    if (docs == null) {
                        cluster = nextQualifiedCluster();
                    } else {
                        int docId = docs.nextDoc();
                        if (docId != DocIdSetIterator.NO_MORE_DOCS) {
                            return docId;
                        }
                        cluster = nextQualifiedCluster();
                    }
                    if (cluster == null) {
                        return DocIdSetIterator.NO_MORE_DOCS;
                    }
                    docs = cluster.getDisi();
                    // every cluster should have at least one doc
                    return docs.nextDoc();
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

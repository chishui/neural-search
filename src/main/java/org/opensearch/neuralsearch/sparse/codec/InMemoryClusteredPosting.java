/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemoryClusteredPosting {
    public static Map<InMemoryKey.IndexKey, Map<BytesRef, PostingClusters>> inMemoryPostings = new HashMap<>();

    @AllArgsConstructor
    public static class InMemoryClusteredPostingReader {
        private final InMemoryKey.IndexKey key;

        public PostingClusters read(BytesRef term) {
            if (!inMemoryPostings.containsKey(key)) {
                return null;
            }
            if (!inMemoryPostings.get(key).containsKey(term)) {
                return null;
            }
            return inMemoryPostings.get(key).get(term);
        }
    }

    public static class InMemoryClusteredPostingWriter extends PushPostingsWriterBase {

        private List<DocFreq> docFreqs = new ArrayList<>();
        private BytesRef currentTerm;
        private final SegmentWriteState state;
        private final PostingClustering postingClustering;

        public InMemoryClusteredPostingWriter(SegmentWriteState state, FieldInfo fieldInfo, PostingClustering postingClustering) {
            super();
            setField(fieldInfo);
            this.state = state;
            this.postingClustering = postingClustering;
        }

        public BlockTermState writeInMemoryTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms)
            throws IOException {
            this.currentTerm = term;
            return super.writeTerm(term, termsEnum, docsSeen, norms);
        }

        @Override
        public BlockTermState newTermState() throws IOException {
            return new Lucene101PostingsFormat.IntBlockTermState();
        }

        @Override
        public void startTerm(NumericDocValues norms) throws IOException {
            docFreqs.clear();
        }

        @Override
        public void finishTerm(BlockTermState state) throws IOException {
            List<DocumentCluster> clusters = this.postingClustering.cluster(docFreqs);
            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(this.state.segmentInfo, this.fieldInfo);
            if (!inMemoryPostings.containsKey(key)) {
                inMemoryPostings.put(key, new HashMap<>());
            }
            inMemoryPostings.get(key).put(this.currentTerm.clone(), new PostingClusters(clusters, clusters.size()));
            this.docFreqs.clear();
            this.currentTerm = null;
        }

        @Override
        public void startDoc(int docID, int freq) throws IOException {
            docFreqs.add(new DocFreq(docID, freq));
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {

        }

        @Override
        public void finishDoc() throws IOException {

        }

        @Override
        public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {

        }

        @Override
        public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}

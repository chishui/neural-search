/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.algorithm.ClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.algorithm.RandomClustering;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.common.DocWeight;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_PRUNE_RATIO;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_MINIMUM_LENGTH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;

/**
 * ClusteredPostingTermsWriter is used to write postings for each segment.
 * It handles the logic to write data to both in-memory and lucene index.
 */
@Log4j2
public class ClusteredPostingTermsWriter extends PushPostingsWriterBase {
    private FixedBitSet docsSeen;
    private IndexOutput postingOut;
    private final List<DocWeight> docWeights = new ArrayList<>();
    private BytesRef currentTerm;
    private PostingClustering postingClustering;
    private InMemoryKey.IndexKey key;
    private SegmentInfo segmentInfo;
    private final int version;
    private final String codec_name;

    public ClusteredPostingTermsWriter(String codec_name, int version) {
        super();
        this.version = version;
        this.codec_name = codec_name;
    }

    public BlockTermState write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
        this.currentTerm = text;
        return super.writeTerm(text, termsEnum, docsSeen, norms);
    }

    public BlockTermState write(BytesRef text, PostingClusters postingClusters) throws IOException {
        this.currentTerm = text;
        BlockTermState state = newTermState();
        writePostingClusters(postingClusters, state);
        return state;
    }

    public void setFieldAndMaxDoc(FieldInfo fieldInfo, int maxDoc) {
        super.setField(fieldInfo);
        key = new InMemoryKey.IndexKey(this.segmentInfo, fieldInfo);
        SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.getOrCreate(key, maxDoc);
        float cluster_ratio = Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
        int nPostings;
        if (Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD)) == DEFAULT_N_POSTINGS) {
            nPostings = Math.max((int) (DEFAULT_POSTING_PRUNE_RATIO * maxDoc), DEFAULT_POSTING_MINIMUM_LENGTH);
        } else {
            nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
        }
        float summaryPruneRatio = Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));
        // TODO: attach a lucene reader in the following CacheGatedForwardIndexReader
        this.postingClustering = new PostingClustering(
            nPostings,
            new RandomClustering(
                summaryPruneRatio,
                cluster_ratio,
                new CacheGatedForwardIndexReader(index == null ? null : index.getReader(), null, null)
            )
        );
    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new Lucene101PostingsFormat.IntBlockTermState();
    }

    @Override
    public void startTerm(NumericDocValues norms) throws IOException {
        docWeights.clear();
    }

    private void writePostingClusters(PostingClusters postingClusters, BlockTermState state) throws IOException {
        List<DocumentCluster> clusters = postingClusters.getClusters();
        // write file
        state.blockFilePointer = postingOut.getFilePointer();
        postingOut.writeVLong(clusters.size());
        for (DocumentCluster cluster : clusters) {
            postingOut.writeVLong(cluster.size());
            Iterator<DocWeight> iterator = cluster.iterator();
            while (iterator.hasNext()) {
                DocWeight docWeight = iterator.next();
                postingOut.writeVInt(docWeight.getDocID());
                postingOut.writeByte(docWeight.getWeight());
            }
            postingOut.writeByte((byte) (cluster.isShouldNotSkip() ? 1 : 0));
            if (cluster.getSummary() == null) {
                postingOut.writeVLong(0);
            } else {
                IteratorWrapper<SparseVector.Item> iter = cluster.getSummary().iterator();
                postingOut.writeVLong(cluster.getSummary().getSize());
                while (iter.hasNext()) {
                    SparseVector.Item item = iter.next();
                    postingOut.writeVInt(item.getToken());
                    postingOut.writeByte(item.getFreq());
                }
            }
        }
    }

    @Override
    public void finishTerm(BlockTermState state) throws IOException {
        PostingClusters postingClusters = new ClusteringTask(this.currentTerm, docWeights, key, this.postingClustering).get();
        writePostingClusters(postingClusters, state);
        this.docWeights.clear();
        this.currentTerm = null;
    }

    @Override
    public void startDoc(int docID, int freq) throws IOException {
        if (docID == -1) {
            throw new IllegalStateException("docId must be set before startDoc");
        }
        docWeights.add(new DocWeight(docID, ByteQuantizer.quantizeFloatToByte(ValueEncoder.decodeFeatureValue(freq))));
    }

    @Override
    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDoc() throws IOException {

    }

    @Override
    public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
        this.postingOut = termsOut;
        this.segmentInfo = state.segmentInfo;
        this.docsSeen = new FixedBitSet(state.segmentInfo.maxDoc());
        CodecUtil.writeIndexHeader(postingOut, this.codec_name, version, state.segmentInfo.getId(), state.segmentSuffix);
    }

    @Override
    public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        CodecUtil.writeFooter(this.postingOut);
    }

    public void closeWithException() {
        IOUtils.closeWhileHandlingException(this.postingOut);
    }

    public void close(long startFp) throws IOException {
        this.postingOut.writeLong(startFp);
        close();
    }
}

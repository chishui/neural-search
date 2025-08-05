/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.codec.SparsePostingsReader;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.common.DocWeight;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class BatchClusteringTask implements Supplier<List<Pair<BytesRef, PostingClusters>>> {
    @Getter
    private final List<BytesRef> terms;
    private final CacheKey key;
    private final float summaryPruneRatio;
    private final float clusterRatio;
    private final int nPostings;
    private final MergeState mergeState;
    private final FieldInfo fieldInfo;

    public BatchClusteringTask(
        List<BytesRef> terms,
        CacheKey key,
        float summaryPruneRatio,
        float clusterRatio,
        int nPostings,
        @NonNull MergeState mergeState,
        FieldInfo fieldInfo
    ) {
        this.terms = terms.stream().map(BytesRef::deepCopyOf).toList();
        this.key = key;
        this.summaryPruneRatio = summaryPruneRatio;
        this.clusterRatio = clusterRatio;
        this.nPostings = nPostings;
        this.mergeState = mergeState;
        this.fieldInfo = fieldInfo;
    }

    @Override
    public List<Pair<BytesRef, PostingClusters>> get() {
        List<Pair<BytesRef, PostingClusters>> postingClusters = new ArrayList<>();
        int maxDocs = getTotalDocs();
        if (maxDocs == 0) {
            return postingClusters;
        }
        try {
            for (BytesRef term : this.terms) {
                int[] newIdToFieldProducerIndex = new int[maxDocs];
                int[] newIdToOldId = new int[maxDocs];
                List<DocWeight> docWeights = SparsePostingsReader.getMergedPostingForATerm(
                    this.mergeState,
                    term,
                    this.fieldInfo,
                    newIdToFieldProducerIndex,
                    newIdToOldId
                );
                PostingClustering postingClustering = new PostingClustering(
                    nPostings,
                    new RandomClustering(summaryPruneRatio, clusterRatio, (newDocId) -> {
                        int oldId = newIdToOldId[newDocId];
                        int segmentIndex = newIdToFieldProducerIndex[newDocId];
                        BinaryDocValues binaryDocValues = mergeState.docValuesProducers[segmentIndex].getBinary(fieldInfo);
                        SparseVectorReader reader = getCacheGatedForwardIndexReader(binaryDocValues);
                        return reader.read(oldId);
                    })
                );
                List<DocumentCluster> clusters = postingClustering.cluster(docWeights);
                postingClusters.add(Pair.of(term, new PostingClusters(clusters)));
                ClusteredPostingWriter writer = ClusteredPostingCache.getInstance().getOrCreate(key).getWriter();
                writer.insert(term, clusters);
            }
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        return postingClusters;
    }

    private int getTotalDocs() {
        int maxDocs = 0;
        for (int i = 0; i < this.mergeState.maxDocs.length; ++i) {
            maxDocs += this.mergeState.maxDocs[i];
        }
        return maxDocs;
    }

    /**
     * Creates a createSparseVectorReader for vector access.
     *
     * @param binaryDocValues binaryDocValues The binary doc values containing sparse vector data
     * @return A CacheGatedForwardIndexReader instance
     */
    private CacheGatedForwardIndexReader getCacheGatedForwardIndexReader(BinaryDocValues binaryDocValues) {
        if (binaryDocValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValues) {
            SegmentInfo segmentInfo = sparseBinaryDocValues.getSegmentInfo();
            CacheKey cacheKey = new CacheKey(segmentInfo, fieldInfo);
            ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
            if (index == null) {
                return new CacheGatedForwardIndexReader(null, null, sparseBinaryDocValues);
            }
            return new CacheGatedForwardIndexReader(index.getReader(), index.getWriter(), sparseBinaryDocValues);
        } else {
            return new CacheGatedForwardIndexReader(null, null, null);
        }
    }
}

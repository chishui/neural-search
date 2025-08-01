/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.common.DocWeight;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;
import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.algorithm.ClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.algorithm.RandomClustering;
import org.opensearch.neuralsearch.sparse.codec.CacheGatedForwardIndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import java.util.List;
import java.util.ArrayList;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_PRUNE_RATIO;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_MINIMUM_LENGTH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * NeuralSparseIndexShard wraps IndexShard and adds methods to perform neural-sparse related operations against the shard
 */
@Log4j2
public class NeuralSparseIndexShard {
    @Getter
    private final IndexShard indexShard;
    // private final NativeMemoryCacheManager nativeMemoryCacheManager;

    /**
     * Constructor to generate NeuralSparseIndexShard. We do not perform validation that the index the shard is from
     * is in fact a neural sparse Index (index.neural_sparse = true). This may make sense to add later, but for now the operations for
     * NeuralSparseIndexShard that are not from a neural-sparse index should be no-ops.
     *
     * @param indexShard IndexShard to be wrapped.
     */
    public NeuralSparseIndexShard(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    /**
     * Return the name of the shards index
     *
     * @return Name of shard's index
     */
    public String getIndexName() {
        return indexShard.shardId().getIndexName();
    }

    /**
     * Load all the neural-sparse segments for this shard into the cache.
     * Preloads sparse field data to improve query performance.
     */
    public void warmUp() throws IOException {
        log.info("[Neural Sparse] Warming up index: [{}]", getIndexName());

        final MapperService mapperService = indexShard.mapperService();

        try (Engine.Searcher searcher = indexShard.acquireSearcher("neural-sparse-warmup")) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse token fields in this segment
                final Set<String> sparseFieldNames = StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
                    .filter(fieldInfo -> fieldInfo.attributes().containsKey(SparseTokensField.SPARSE_FIELD))
                    .filter(fieldInfo -> {
                        final MappedFieldType fieldType = mapperService.fieldType(fieldInfo.getName());
                        return fieldType instanceof SparseTokensFieldType;
                    })
                    .map(FieldInfo::getName)
                    .collect(Collectors.toSet());

                log.info("[Neural Sparse] Warming up sparse fields: {} in segment", sparseFieldNames);

                // Preload sparse field data by reading all binary doc values
                for (String fieldName : sparseFieldNames) {
                    try {
                        final FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(fieldName);
                        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                            // Actually read the binary doc values to trigger cache loading
                            final BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldName);
                            if (binaryDocValues != null) {
                                // Create cache key and check if already cached
                                final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                                final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
                                final InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(segmentInfo, fieldName);

                                // Check if both forward index and clustered posting are already cached
                                if (InMemorySparseVectorForwardIndex.get(key) != null && InMemoryClusteredPosting.get(key) != null) {
                                    log.info(
                                        "[Neural Sparse] Cache already exists for field: {} in segment: {}, skipping",
                                        fieldName,
                                        segmentInfo.name
                                    );
                                    continue;
                                }

                                final int docCount = segmentInfo.maxDoc();
                                final SparseVectorWriter writer = InMemorySparseVectorForwardIndex.getOrCreate(key, docCount).getWriter();

                                // Read all documents to populate forward index cache
                                int docId = binaryDocValues.nextDoc();
                                int loadedDocs = 0;
                                while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                                    final BytesRef bytesRef = binaryDocValues.binaryValue();
                                    writer.insert(docId, new SparseVector(bytesRef));
                                    loadedDocs++;
                                    docId = binaryDocValues.nextDoc();
                                }

                                // Now trigger clustering to populate inverted index cache
                                warmUpClusteredPostings(leafReader, fieldInfo, key);

                                log.info(
                                    "[Neural Sparse] Loaded {} documents for field: {} in segment: {}",
                                    loadedDocs,
                                    fieldName,
                                    segmentInfo.name
                                );
                            }
                        }
                    } catch (Exception e) {
                        log.warn("[Neural Sparse] Failed to warm up field: {}", fieldName, e);
                    }
                }
            }
        }

        log.info("[Neural Sparse] Completed warming up index: [{}]", getIndexName());
    }

    /**
     * Warm up clustered postings (inverted index) for a specific field
     */
    private void warmUpClusteredPostings(LeafReader leafReader, FieldInfo fieldInfo, InMemoryKey.IndexKey key) throws IOException {
        // Get terms for this field to trigger clustering
        final Terms terms = leafReader.terms(fieldInfo.name);
        if (terms == null) {
            return;
        }

        // Create PostingClustering with field attributes
        final int maxDoc = leafReader.maxDoc();
        float clusterRatio = Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
        int nPostings;
        if (Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD)) == DEFAULT_N_POSTINGS) {
            nPostings = Math.max((int) (DEFAULT_POSTING_PRUNE_RATIO * maxDoc), DEFAULT_POSTING_MINIMUM_LENGTH);
        } else {
            nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
        }
        final float summaryPruneRatio = Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));
        int clusterUtilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(APPROXIMATE_THRESHOLD_FIELD));

        if (clusterUtilDocCountReach > 0 && maxDoc < clusterUtilDocCountReach) {
            clusterRatio = 0;
        }

        final PostingClustering postingClustering = new PostingClustering(
            nPostings,
            new RandomClustering(
                summaryPruneRatio,
                clusterRatio,
                new CacheGatedForwardIndexReader(InMemorySparseVectorForwardIndex.get(key).getReader(), null, null)
            )
        );

        // Iterate through all terms and trigger clustering
        final TermsEnum termsEnum = terms.iterator();
        int clusteredTerms = 0;
        BytesRef term;
        while ((term = termsEnum.next()) != null) {
            // Collect DocWeight for this term
            final List<DocWeight> docWeights = new ArrayList<>();
            final var postingsEnum = termsEnum.postings(null);
            int docId;
            while ((docId = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                final int freq = postingsEnum.freq();
                docWeights.add(new DocWeight(docId, ByteQuantizer.quantizeFloatToByte(ValueEncoder.decodeFeatureValue(freq))));
            }

            // Trigger clustering for this term
            if (!docWeights.isEmpty()) {
                new ClusteringTask(term, docWeights, key, postingClustering).get();
                clusteredTerms++;
            }
        }

        log.info("[Neural Sparse] Clustered {} terms for field: {}", clusteredTerms, fieldInfo.name);
    }

    /**
     * Clear all cached neural-sparse data for this shard.
     * Removes sparse field data from memory to free up resources.
     */
    public void clearCache() {
        final String indexName = getIndexName();
        log.info("[Neural Sparse] Clearing cache for index: [{}]", indexName);

        try (Engine.Searcher searcher = indexShard.acquireSearcher("neural-sparse-clear-cache")) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse token fields in this segment
                final Set<String> sparseFieldNames = StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
                    .filter(fieldInfo -> fieldInfo.attributes().containsKey(SparseTokensField.SPARSE_FIELD))
                    .map(FieldInfo::getName)
                    .collect(Collectors.toSet());

                // Get segment info for creating cache keys
                final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;

                // Clear in-memory cache for each sparse field
                for (String fieldName : sparseFieldNames) {
                    try {
                        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, fieldName);
                        InMemoryClusteredPosting.clearIndex(indexKey);
                        InMemorySparseVectorForwardIndex.removeIndex(indexKey);
                        log.debug("[Neural Sparse] Cleared cache for field: {} in segment: {}", fieldName, segmentInfo.name);
                    } catch (Exception e) {
                        log.warn("[Neural Sparse] Failed to clear cache for field: {} in segment: {}", fieldName, segmentInfo.name, e);
                    }
                }

                log.info("[Neural Sparse] Cleared cache for sparse fields: {} in segment", sparseFieldNames);
            }
        } catch (Exception e) {
            log.error("[Neural Sparse] Failed to clear cache for index: [{}]", indexName, e);
            throw new RuntimeException(e);
        }

        log.info("[Neural Sparse] Completed clearing cache for index: [{}]", indexName);
    }

}

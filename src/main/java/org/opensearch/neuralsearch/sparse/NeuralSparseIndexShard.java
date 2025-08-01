/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.codec.InMemorySparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

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

                // Preload sparse field data by accessing binary doc values
                for (String fieldName : sparseFieldNames) {
                    try {
                        final FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(fieldName);
                        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                            // Access binary doc values to trigger loading
                            leafReader.getBinaryDocValues(fieldName);
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

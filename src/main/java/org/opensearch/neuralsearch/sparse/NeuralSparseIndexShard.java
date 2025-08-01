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
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.knn.common.KNNConstants.VECTOR_DATA_TYPE_FIELD;

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
     * First it tries to warm-up memory optimized fields, then load off-heap fields.
     *
     * @throws IOException Thrown when getting the SEISMIC Paths to be loaded in
     */
    public void warmup() throws IOException {
        log.info("[Neural Sparse] Warming up index: [{}]", getIndexName());

        final MapperService mapperService = indexShard.mapperService();
        final String indexName = indexShard.shardId().getIndexName();
        final Directory directory = indexShard.store().directory();

        try (Engine.Searcher searcher = indexShard.acquireSearcher("neural-sparse-warmup-mem")) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                // Load memory optimized searcher in a single segment first.
                final Set<String> loadedFieldNames = warmUpMemoryOptimizedSearcher(leafReaderContext.reader(), mapperService, indexName);
                log.info("[KNN] Loaded memory optimized searchers for fields {}", loadedFieldNames);

                // Load off-heap index
                final List<KNNIndexShard.EngineFileContext> engineFileContexts = getAllEngineFileContexts(loadedFieldNames, leafReaderContext);
                warmUpOffHeapIndex(engineFileContexts, directory);
                log.info("[KNN] Loaded off-heap indices for fields {}", engineFileContexts.stream().map(ctx -> ctx.fieldName));
            }
        }
    }

    private Set<String> warmUpMemoryOptimizedSearcher(
            final LeafReader leafReader,
            final MapperService mapperService,
            final String indexName
    ) {

        final Set<FieldInfo> fieldsForMemoryOptimizedSearch = StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
                .filter(fieldInfo -> fieldInfo.attributes().containsKey(KNNVectorFieldMapper.KNN_FIELD))
                .filter(fieldInfo -> {
                    final MappedFieldType fieldType = mapperService.fieldType(fieldInfo.getName());

                    if (fieldType instanceof KNNVectorFieldType knnFieldType) {
                        return MemoryOptimizedSearchSupportSpec.isSupportedFieldType(knnFieldType, indexName);
                    }

                    return false;
                })
                .collect(Collectors.toSet());

        final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
        for (final FieldInfo field : fieldsForMemoryOptimizedSearch) {
            final String dataTypeStr = field.getAttribute(VECTOR_DATA_TYPE_FIELD);
            if (dataTypeStr == null) {
                continue;
            }
            try {
                // Partial load Faiss index by triggering search.
                final VectorDataType vectorDataType = VectorDataType.get(dataTypeStr);
                if (vectorDataType == VectorDataType.FLOAT) {
                    segmentReader.getVectorReader().search(field.getName(), (float[]) null, null, null);
                } else {
                    segmentReader.getVectorReader().search(field.getName(), (byte[]) null, null, null);
                }
            } catch (Exception e) {
                // Ignore
            }
        }

        return fieldsForMemoryOptimizedSearch.stream().map(FieldInfo::getName).collect(Collectors.toSet());
    }

}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.codec.SparseTermsLuceneReader;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.apache.lucene.index.SegmentReadState;
import java.util.Set;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * NeuralSparseIndexShard wraps IndexShard and adds methods to perform neural-sparse related operations against the shard
 */
@Log4j2
@RequiredArgsConstructor
public class NeuralSparseIndexShard {
    @Getter
    @NonNull
    private final IndexShard indexShard;

    private static final String warmUpSearcherSource = "warm-up-searcher-source";
    private static final String clearCacheSearcherSource = "clear-cache-searcher-source";

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

        try (Engine.Searcher searcher = indexShard.acquireSearcher(warmUpSearcherSource)) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse token fields in this segment
                final Set<String> sparseFieldNames = collectSparseTokenFields(leafReader);

                log.info("[Neural Sparse] Warming up sparse fields: {} in segment", sparseFieldNames);

                // Use CacheGated readers to automatically populate cache on read
                for (String fieldName : sparseFieldNames) {
                    try {
                        final FieldInfo fieldInfo = leafReader.getFieldInfos().fieldInfo(fieldName);
                        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                            final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                            final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
                            final CacheKey key = new CacheKey(segmentInfo, fieldName);

                            if (!PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
                                continue;
                            }

                            warmUpWithCacheGatedReaders(leafReader, fieldInfo, key);
                        }
                    } catch (Exception e) {
                        log.warn("[Neural Sparse] Failed to warm up field: {}", fieldName, e);
                    }
                }
            }
        }
    }

    /**
     * Clear all cached neural-sparse data for this shard.
     * Removes sparse field data from memory to free up resources.
     */
    public void clearCache() {
        final String indexName = getIndexName();
        log.info("[Neural Sparse] Clearing cache for index: [{}]", indexName);

        try (Engine.Searcher searcher = indexShard.acquireSearcher(clearCacheSearcherSource)) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse token fields in this segment
                final Set<String> sparseFieldNames = collectSparseTokenFields(leafReader);

                // Get segment info for creating cache keys
                final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;

                // Clear in-memory cache for each sparse field
                for (String fieldName : sparseFieldNames) {
                    try {
                        CacheKey indexKey = new CacheKey(segmentInfo, fieldName);
                        ClusteredPostingCache.getInstance().removeIndex(indexKey);
                        ForwardIndexCache.getInstance().removeIndex(indexKey);
                        log.debug("[Neural Sparse] Cleared cache for field: {} in segment: {}", fieldName, segmentInfo.name);
                    } catch (Exception e) {
                        log.warn("[Neural Sparse] Failed to clear cache for field: {} in segment: {}", fieldName, segmentInfo.name, e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("[Neural Sparse] Failed to clear cache for index: [{}]", indexName, e);
            throw new RuntimeException(e);
        }

        log.info("[Neural Sparse] Completed clearing cache for index: [{}]", indexName);
    }

    /**
     * Warm up using CacheGated readers that automatically populate cache on read
     */
    private void warmUpWithCacheGatedReaders(LeafReader leafReader, FieldInfo fieldInfo, CacheKey key) throws IOException {
        final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
        final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
        final int docCount = segmentInfo.maxDoc();
        final BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldInfo.name);
        if (binaryDocValues == null) {
            log.warn("[Neural Sparse] No binary doc values found for field: {}", fieldInfo.name);
            return;
        }
        try {
            // Create SegmentReadState for SparseTermsLuceneReader
            final SegmentReadState readState = new SegmentReadState(
                segmentInfo.dir,
                segmentInfo,
                leafReader.getFieldInfos(),
                IOContext.DEFAULT
            );

            // Create SparseTermsLuceneReader for inverted index
            final SparseTermsLuceneReader luceneReader = new SparseTermsLuceneReader(readState);

            final CacheGatedPostingsReader postingsReader = new CacheGatedPostingsReader(
                fieldInfo.name,
                ClusteredPostingCache.getInstance().getOrCreate(key).getReader(),
                ClusteredPostingCache.getInstance().getOrCreate(key).getWriter(),
                luceneReader
            );

            warmUpInvertedIndex(postingsReader);

        } catch (Exception e) {
            log.warn("[Neural Sparse] Failed to create Lucene reader for inverted index. Will only warm up forward index next", e);
        }

        try {
            // Create CacheGated readers
            final CacheGatedForwardIndexReader forwardIndexReader = getCacheGatedForwardIndexReader(binaryDocValues, key, docCount);
            warmUpForwardIndex(binaryDocValues, forwardIndexReader);
        } catch (Exception e) {
            log.warn("[Neural Sparse] Failed to warm up forward index", e);
        }
    }

    /**
     * Create CacheGatedForwardIndexReader following the existing pattern
     */
    private CacheGatedForwardIndexReader getCacheGatedForwardIndexReader(BinaryDocValues binaryDocValues, CacheKey key, int docCount) {
        if (binaryDocValues instanceof SparseBinaryDocValuesPassThrough sparseBinaryDocValues) {
            ForwardIndexCacheItem index = ForwardIndexCache.getInstance().getOrCreate(key, docCount);
            return new CacheGatedForwardIndexReader(null, index.getWriter(), sparseBinaryDocValues);
        } else {
            // For regular BinaryDocValues, create a SparseVectorReader wrapper
            final SparseVectorReader luceneReader = docId -> {
                try {
                    if (binaryDocValues.advanceExact(docId)) {
                        return new SparseVector(binaryDocValues.binaryValue());
                    }
                } catch (IOException e) {
                    log.warn("Failed to read doc {}", docId, e);
                }
                return null;
            };

            ForwardIndexCacheItem index = ForwardIndexCache.getInstance().getOrCreate(key, docCount);
            return new CacheGatedForwardIndexReader(null, index.getWriter(), luceneReader);
        }
    }

    private Set<String> collectSparseTokenFields(LeafReader leafReader) {
        return StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
            .filter(SparseTokensField::isSparseField)
            .map(FieldInfo::getName)
            .collect(Collectors.toSet());
    }

    private void warmUpForwardIndex(BinaryDocValues binaryDocValues, CacheGatedForwardIndexReader cacheGatedForwardIndexReader)
        throws IOException {
        int loadedDocs = 0;
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            try {
                if (cacheGatedForwardIndexReader.read(docId) != null) {
                    loadedDocs++;
                }
            } catch (IOException e) {
                log.warn("[Neural Sparse] Failed to read doc {} during warm up", docId, e);
            }
            docId = binaryDocValues.nextDoc();
        }
        log.info("[Neural Sparse] Warmed up {} docs in total", loadedDocs);
    }

    private void warmUpInvertedIndex(CacheGatedPostingsReader cacheGatedPostingsReader) {
        int loadedTerms = 0;
        final Set<BytesRef> terms = cacheGatedPostingsReader.getTerms();
        for (BytesRef term : terms) {
            try {
                if (cacheGatedPostingsReader.read(term) != null) {
                    loadedTerms++;
                }
            } catch (IOException e) {
                log.warn("[Neural Sparse] Failed to read term {} during warm up", term, e);
            }
        }
        log.info("[Neural Sparse] Warmed up {} terms in total", loadedTerms);
    }

}

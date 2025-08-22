/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.shard.IllegalIndexShardStateException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.codec.SparseTermsLuceneReader;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensField;

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

    private static final String WARM_UP_SEARCHER_SOURCE = "warm-up-searcher-source";
    private static final String CLEAR_CACHE_SEARCHER_SOURCE = "clear-cache-searcher-source";

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
     * Uses read lock to allow concurrent warmup operations but prevent conflicts with clear cache.
     * Early stop to save resources if this is a repeated request
     */
    public void warmUp() throws IOException {
        try (Engine.Searcher searcher = indexShard.acquireSearcher(WARM_UP_SEARCHER_SOURCE)) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse FieldInfos in this segment
                final Set<FieldInfo> sparseFieldInfos = collectSparseFieldInfos(leafReader);

                // Use CacheGated readers to automatically populate cache on read
                for (FieldInfo fieldInfo : sparseFieldInfos) {
                    try {
                        final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                        final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
                        final CacheKey key = new CacheKey(segmentInfo, fieldInfo);

                        if (!PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
                            continue;
                        }
                        warmUpWithCacheGatedReaders(leafReader, fieldInfo, key);

                    } catch (CircuitBreakingException e) {
                        log.error("[Neural Sparse] Circuit Breaker reaches limit", e);
                        throw e;
                    } catch (IOException e) {
                        log.error("[Neural Sparse] Failed to read data during warm up", e);
                        throw e;
                    }
                }
            }
        } catch (IllegalIndexShardStateException | EngineException e) {
            log.error("[Neural Sparse] Failed to acquire searcher", e);
            throw e;
        }
    }

    /**
     * Clear all cached neural-sparse data for this shard.
     * Removes sparse field data from memory to free up resources.
     * Uses write lock to ensure exclusive access during cache clearing.
     */
    public void clearCache() {
        try (Engine.Searcher searcher = indexShard.acquireSearcher(CLEAR_CACHE_SEARCHER_SOURCE)) {
            for (final LeafReaderContext leafReaderContext : searcher.getIndexReader().leaves()) {
                final LeafReader leafReader = leafReaderContext.reader();

                // Find all sparse token fields in this segment
                final Set<FieldInfo> sparseFieldInfos = collectSparseFieldInfos(leafReader);

                // Get segment info for creating cache keys
                final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
                final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;

                // Clear in-memory cache for each sparse field
                for (FieldInfo fieldInfo : sparseFieldInfos) {
                    if (!PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
                        continue;
                    }
                    CacheKey cacheKey = new CacheKey(segmentInfo, fieldInfo);
                    ClusteredPostingCache.getInstance().removeIndex(cacheKey);
                    ForwardIndexCache.getInstance().removeIndex(cacheKey);
                }
            }
        } catch (IllegalIndexShardStateException | EngineException e) {
            log.error("[Neural Sparse] Failed to acquire searcher", e);
            throw e;
        }
    }

    /**
     * Warm up using CacheGated readers that automatically populate cache on read
     */
    private void warmUpWithCacheGatedReaders(LeafReader leafReader, FieldInfo fieldInfo, CacheKey key) throws IOException,
        CircuitBreakingException {
        final SegmentReader segmentReader = Lucene.segmentReader(leafReader);
        final SegmentInfo segmentInfo = segmentReader.getSegmentInfo().info;
        final int docCount = segmentInfo.maxDoc();
        final BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldInfo.name);
        if (binaryDocValues == null) {
            log.error("[Neural Sparse] No binary doc values found for field: {}", fieldInfo.name);
            return;
        }

        final CacheGatedForwardIndexReader forwardIndexReader = getCacheGatedForwardIndexReader(binaryDocValues, key, docCount);
        warmUpForwardIndex(binaryDocValues, forwardIndexReader);

        final CacheGatedPostingsReader postingsReader = getCacheGatedPostingReader(fieldInfo, key, segmentInfo);
        warmUpInvertedIndex(postingsReader);
    }

    private CacheGatedForwardIndexReader getCacheGatedForwardIndexReader(BinaryDocValues binaryDocValues, CacheKey key, int docCount) {
        assert (binaryDocValues instanceof SparseBinaryDocValuesPassThrough);
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues = (SparseBinaryDocValuesPassThrough) binaryDocValues;
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(key, docCount);
        return new CacheGatedForwardIndexReader(
            cacheItem.getReader(),
            cacheItem.getWriter(this::customizedConsumer),
            sparseBinaryDocValues
        );
    }

    private CacheGatedPostingsReader getCacheGatedPostingReader(FieldInfo fieldInfo, CacheKey key, SegmentInfo segmentInfo)
        throws IOException {
        final SparseTermsLuceneReader luceneReader = new SparseTermsLuceneReader(createSegmentReadState(segmentInfo));
        return new CacheGatedPostingsReader(
            fieldInfo.name,
            ClusteredPostingCache.getInstance().getOrCreate(key).getReader(),
            ClusteredPostingCache.getInstance().getOrCreate(key).getWriter(this::customizedConsumer),
            luceneReader
        );
    }

    private void customizedConsumer(long ramBytesUsed) {
        throw new CircuitBreakingException("Circuit Breaker reaches limit", CircuitBreaker.Durability.PERMANENT);
    }

    private Set<FieldInfo> collectSparseFieldInfos(LeafReader leafReader) {
        return StreamSupport.stream(leafReader.getFieldInfos().spliterator(), false)
            .filter(SparseTokensField::isSparseField)
            .collect(Collectors.toSet());
    }

    private void warmUpForwardIndex(BinaryDocValues binaryDocValues, CacheGatedForwardIndexReader cacheGatedForwardIndexReader)
        throws IOException, CircuitBreakingException {
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            cacheGatedForwardIndexReader.read(docId);
            docId = binaryDocValues.nextDoc();
        }
    }

    private void warmUpInvertedIndex(CacheGatedPostingsReader cacheGatedPostingsReader) throws IOException, CircuitBreakingException {
        final Set<BytesRef> terms = cacheGatedPostingsReader.getTerms();
        for (BytesRef term : terms) {
            cacheGatedPostingsReader.read(term);
        }
    }

    private SegmentReadState createSegmentReadState(SegmentInfo segmentInfo) throws IOException {
        final Codec codec = segmentInfo.getCodec();
        final Directory cfsDir;
        final FieldInfos coreFieldInfos;

        if (segmentInfo.getUseCompoundFile()) {
            // If we get compound file, we will set directory as csf file
            cfsDir = codec.compoundFormat().getCompoundReader(segmentInfo.dir, segmentInfo);
        } else {
            /*Otherwise, we set directory as dir coming from segmentInfo*/
            cfsDir = segmentInfo.dir;
        }
        coreFieldInfos = codec.fieldInfosFormat().read(cfsDir, segmentInfo, "", IOContext.DEFAULT);

        return new SegmentReadState(cfsDir, segmentInfo, coreFieldInfos, IOContext.DEFAULT);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class manages the cache postings for sparse vectors. It provides methods to write and read postings from cache.
 * It is used by the SparsePostingsConsumer and SparsePostingsReader classes.
 */
@Log4j2
public class ClusteredPostingCacheItem implements Accountable {

    private static final String CIRCUIT_BREAKER_LABEL = "Cache Clustered Posting";
    private final Map<BytesRef, PostingClusters> clusteredPostings = new ConcurrentHashMap<>();
    private final AtomicLong usedRamBytes = new AtomicLong(RamUsageEstimator.shallowSizeOf(clusteredPostings));
    @Getter
    private final ClusteredPostingReader reader = new CacheClusteredPostingReader();
    @Getter
    private final ClusteredPostingWriter writer = new CacheClusteredPostingWriter();

    public ClusteredPostingCacheItem() {
        CircuitBreakerManager.addWithoutBreaking(usedRamBytes.get());
    }

    @Override
    public long ramBytesUsed() {
        return usedRamBytes.get();
    }

    private class CacheClusteredPostingReader implements ClusteredPostingReader {
        @Override
        public PostingClusters read(BytesRef term) {
            return clusteredPostings.get(term);
        }

        @Override
        public Set<BytesRef> getTerms() {
            // Note: We're returning the keySet directly instead of using Collections.unmodifiableSet()
            // for performance reasons. Callers should treat this as a read-only view.
            return clusteredPostings.keySet();
        }

        @Override
        public long size() {
            return clusteredPostings.size();
        }
    }

    private class CacheClusteredPostingWriter implements ClusteredPostingWriter {
        public void insert(BytesRef term, List<DocumentCluster> clusters) {
            if (clusters == null || clusters.isEmpty() || term == null) {
                return;
            }

            // Clone a new BytesRef object to avoid offset change
            BytesRef clonedTerm = term.clone();
            PostingClusters postingClusters = new PostingClusters(clusters);
            // BytesRef.bytes is never null
            long ramBytesUsed = postingClusters.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(clonedTerm) + clonedTerm.bytes.length;

            if (!CircuitBreakerManager.addMemoryUsage(ramBytesUsed, CIRCUIT_BREAKER_LABEL)) {
                // TODO: cache eviction
                return;
            }

            // Update the clusters with putIfAbsent for thread safety
            PostingClusters existingClusters = clusteredPostings.putIfAbsent(clonedTerm, postingClusters);

            // Only update memory usage if we actually inserted a new entry
            if (existingClusters == null) {
                usedRamBytes.addAndGet(ramBytesUsed);
            }
        }
    }
}

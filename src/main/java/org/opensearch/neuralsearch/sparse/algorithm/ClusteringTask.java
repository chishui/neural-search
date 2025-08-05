/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.common.DocWeight;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class ClusteringTask implements Supplier<PostingClusters> {
    private final BytesRef term;
    private final List<DocWeight> docs;
    private final PostingClustering postingClustering;
    private final CacheKey key;

    public ClusteringTask(BytesRef term, Collection<DocWeight> docs, CacheKey key, PostingClustering postingClustering) {
        this.docs = docs.stream().toList();
        this.term = BytesRef.deepCopyOf(term);
        this.key = key;
        this.postingClustering = postingClustering;
    }

    @Override
    public PostingClusters get() {
        List<DocumentCluster> clusters;
        try {
            clusters = postingClustering.cluster(this.docs);
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        ClusteredPostingWriter writer = ClusteredPostingCache.getInstance().getOrCreate(key).getWriter();
        writer.insert(term, clusters);
        return new PostingClusters(clusters);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Set;

public class CacheGatedPostingsReader implements ClusteredPostingReader {
    private final String fieldName;
    private final ClusteredPostingReader inMemoryReader;
    private final InMemoryKey.IndexKey indexKey;
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseTermsLuceneReader luceneReader;

    public CacheGatedPostingsReader(
        String fieldName,
        ClusteredPostingReader reader,
        SparseTermsLuceneReader luceneReader,
        InMemoryKey.IndexKey indexKey
    ) {
        this.fieldName = fieldName;
        this.inMemoryReader = reader;
        this.luceneReader = luceneReader;
        this.indexKey = indexKey;
    }

    @Override
    public PostingClusters read(BytesRef term) throws IOException {
        PostingClusters clusters = inMemoryReader.read(term);
        // if cluster does not exist in cache, read from lucene and populate it to cache
        if (clusters == null) {
            clusters = luceneReader.read(fieldName, term);
            if (clusters != null) {
                ClusteredPostingWriter writer = InMemoryClusteredPosting.getOrCreate(indexKey).getWriter();
                writer.write(term, clusters.getClusters());
            }
        }
        return clusters;
    }

    // we return terms from lucene as cache may not have all data due to memory constraint
    @Override
    public Set<BytesRef> getTerms() {
        return luceneReader.getTerms(fieldName);
    }

    @Override
    public long size() {
        return luceneReader.getTerms(fieldName).size();
    }
}

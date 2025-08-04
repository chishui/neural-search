/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;

import java.io.IOException;
import java.util.Set;

public class CacheGatedPostingsReader implements ClusteredPostingReader {
    private final String fieldName;
    private final ClusteredPostingReader inMemoryReader;
    private final ClusteredPostingWriter inMemoryWriter;
    // SparseTermsLuceneReader to read sparse terms from disk
    private final SparseTermsLuceneReader luceneReader;

    public CacheGatedPostingsReader(
        @NonNull String fieldName,
        @NonNull ClusteredPostingReader inMemoryReader,
        @NonNull ClusteredPostingWriter inMemoryWriter,
        @NonNull SparseTermsLuceneReader luceneReader
    ) {
        this.fieldName = fieldName;
        this.inMemoryReader = inMemoryReader;
        this.inMemoryWriter = inMemoryWriter;
        this.luceneReader = luceneReader;
    }

    @Override
    public PostingClusters read(BytesRef term) throws IOException {
        PostingClusters clusters = inMemoryReader.read(term);
        if (clusters != null) {
            return clusters;
        }
        // if cluster does not exist in cache, read from lucene and populate it to cache
        synchronized (luceneReader) {
            clusters = luceneReader.read(fieldName, term);
        }

        if (clusters != null) {
            inMemoryWriter.insert(term, clusters.getClusters());
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

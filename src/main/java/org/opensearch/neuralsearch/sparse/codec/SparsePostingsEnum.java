/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;

public class SparsePostingsEnum extends PostingsEnum {
    private final PostingClusters clusters;
    @Getter
    private final InMemoryKey.IndexKey indexKey;
    private int currentCluster = -1;

    public SparsePostingsEnum(PostingClusters clusters, InMemoryKey.IndexKey indexKey) {
        this.clusters = clusters;
        this.indexKey = indexKey;
    }

    public DocumentCluster nextCluster() {
        if (currentCluster == NO_MORE_DOCS) {
            return null;
        }
        if (currentCluster + 1 >= clusters.getClusters().size()) {
            currentCluster = NO_MORE_DOCS;
            return null;
        }
        ++currentCluster;
        return clusters.getClusters().get(currentCluster);
    }

    @Override
    public int freq() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef getPayload() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int docID() {
        if (currentCluster == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        if (currentCluster == -1) {
            return -1;
        }
        return clusters.getClusters().get(currentCluster).getDisi().docID();
    }

    @Override
    public int nextDoc() throws IOException {
        // call nextCluster before start to call nextDoc
        if (currentCluster == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
        }
        if (currentCluster == -1) {
            return -1;
        }
        return clusters.getClusters().get(currentCluster).getDisi().nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
        return 0;
    }
}

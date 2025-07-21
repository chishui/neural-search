/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;

import java.util.List;

/**
 * Functional interface for writing clustered posting lists.
 * This interface provides a mechanism to write term-based clustered documents
 * to the underlying storage format used by the clustered posting implementation.
 */
@FunctionalInterface
public interface ClusteredPostingWriter {

    /**
     * Writes a term and its associated document clusters to the posting list.
     *
     * @param term The term for which document clusters are being written, represented as a BytesRef
     * @param clusters A list of DocumentCluster objects containing the documents and their associated
     *                data that are relevant for this term
     */
    void write(BytesRef term, List<DocumentCluster> clusters);
}

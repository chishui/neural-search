/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Interface for writing sparse vector data.
 * This interface provides methods to store sparse vector representations
 * associated with document IDs, supporting both high-level SparseVector objects
 * and low-level binary data representations.
 *
 * Implementations of this interface are responsible for efficiently persisting
 * sparse vector data, which may involve writing to disk, memory, or other storage media.
 */
public interface SparseVectorWriter {

    /**
     * Writes a sparse vector object for the specified document ID.
     * This method stores the complete sparse vector representation including
     * indices and values in the underlying storage system.
     *
     * @param docId The document identifier to associate with the sparse vector
     * @param vector The SparseVector object containing the vector data to store
     * @throws IOException If an error occurs during the writing operation, such as
     *                     when accessing the underlying storage medium
     */
    void write(int docId, SparseVector vector) throws IOException;

    /**
     * Writes raw binary data representing a sparse vector for the specified document ID.
     * This method allows for storing pre-serialized sparse vector data, which can be
     * more efficient when the vector is already in binary format.
     *
     * @param docId The document identifier to associate with the sparse vector data
     * @param data A BytesRef containing the serialized sparse vector data
     * @throws IOException If an error occurs during the writing operation, such as
     *                     when accessing the underlying storage medium
     */
    void write(int docId, BytesRef data) throws IOException;
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common.exception;

import java.util.List;

public class NeuralSparseInvalidIndicesException extends RuntimeException {

    private final List<String> invalidIndices;

    public NeuralSparseInvalidIndicesException(List<String> invalidIndices, String message) {
        super(message);
        this.invalidIndices = invalidIndices;
    }

    /**
     * Returns the Invalid Index
     *
     * @return invalid index name
     */
    public List<String> getInvalidIndices() {
        return invalidIndices;
    }

    @Override
    public String toString() {
        return "[Neural Sparse] " + String.join(",", invalidIndices) + ' ' + super.toString();
    }
}

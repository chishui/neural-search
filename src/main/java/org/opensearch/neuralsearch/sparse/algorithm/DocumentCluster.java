/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class DocumentCluster {
    private SparseVector summary;
    private DocIdSetIterator disi;
    // if true, docs in this cluster should always be examined
    private boolean shouldNotSkip;
}

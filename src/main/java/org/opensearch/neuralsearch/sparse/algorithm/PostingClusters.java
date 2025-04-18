/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.neuralsearch.sparse.common.SortedDocInClusterIterator;

import java.util.Iterator;
import java.util.List;

@AllArgsConstructor
@Getter
@Setter
public class PostingClusters implements Iterator<DocumentCluster> {
    private List<DocumentCluster> clusters;
    private int current = -1;

    @Override
    public boolean hasNext() {
        return current + 1 < clusters.size();
    }

    @Override
    public DocumentCluster next() {
        ++current;
        return clusters.get(current);
    }

    public void reset() {
        current = -1;
        for (DocumentCluster cluster : clusters) {
            ((SortedDocInClusterIterator) cluster.getDisi()).reset();
        }
    }
}

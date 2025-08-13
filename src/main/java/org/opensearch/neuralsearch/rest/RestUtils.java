/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A Utils class for REST API operations
 */
@Log4j2
public class RestUtils {
    /**
     * @param indices An array of indices related to the request
     * @param clusterService ClusterService of OpenSearch Cluster
     * @param sparse_index Sparse Index name of setting
     * @param apiOperation Determine whether the request is to warm up or clear cache
     */
    public static void validateIndices(Index[] indices, ClusterService clusterService, String sparse_index, String apiOperation) {
        List<String> invalidIndexNames = Arrays.stream(indices).filter(index -> {
            String sparseIndexSetting = Optional.ofNullable(clusterService)
                .map(cs -> cs.state())
                .map(state -> state.metadata())
                .map(metadata -> metadata.getIndexSafe(index))
                .map(indexMetadata -> indexMetadata.getSettings())
                .map(settings -> settings.get(sparse_index))
                .orElse(null);

            return !"true".equals(sparseIndexSetting);
        }).map(Index::getName).collect(Collectors.toList());

        if (!invalidIndexNames.isEmpty()) {
            throw new NeuralSparseInvalidIndicesException(
                invalidIndexNames,
                String.format(Locale.ROOT, "Request rejected. Indices %s don't support %s operation.", invalidIndexNames, apiOperation)
            );
        }
    }
}

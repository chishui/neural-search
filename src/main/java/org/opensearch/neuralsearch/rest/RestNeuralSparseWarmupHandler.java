/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.apache.commons.lang.StringUtils;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupAction;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupRequest;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_SPARSE_INDEX;
import static org.opensearch.action.support.IndicesOptions.strictExpandOpen;

/**
 * RestHandler for neural-sparse index warmup API. API provides the ability for a user to load specific indices' neural-sparse index
 * into memory.
 */
public class RestNeuralSparseWarmupHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestNeuralSparseWarmupHandler.class);
    private static final String URL_PATH = "/warmup/{index}";
    public static String NAME = "neural_sparse_warmup_action";
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ClusterService clusterService;

    public RestNeuralSparseWarmupHandler(
        Settings settings,
        RestController controller,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(new Route(RestRequest.Method.GET, NeuralSearch.NEURAL_BASE_URI + URL_PATH));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        NeuralSparseWarmupRequest neuralSparseWarmupRequest = createNeuralSparseWarmupRequest(request);
        logger.info("[Neural Sparse] Warmup started for the following indices: " + String.join(",", neuralSparseWarmupRequest.indices()));
        return channel -> client.execute(
            NeuralSparseWarmupAction.INSTANCE,
            neuralSparseWarmupRequest,
            new RestToXContentListener<>(channel)
        );
    }

    private NeuralSparseWarmupRequest createNeuralSparseWarmupRequest(RestRequest request) {
        String[] indexNames = StringUtils.split(request.param("index"), ",");
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), strictExpandOpen(), indexNames);
        List<String> invalidIndexNames = new ArrayList<>();

        Arrays.stream(indices).forEach(index -> {
            if (!"true".equals(clusterService.state().metadata().getIndexSafe(index).getSettings().get(NEURAL_SPARSE_INDEX))) {
                invalidIndexNames.add(index.getName());
            }
        });

        if (invalidIndexNames.size() != 0) {
            throw new NeuralSparseInvalidIndicesException(
                invalidIndexNames,
                "Warm up request rejected. One or more indices have 'index.neural_sparse' set to false."
            );
        }

        return new NeuralSparseWarmupRequest(indexNames);
    }
}

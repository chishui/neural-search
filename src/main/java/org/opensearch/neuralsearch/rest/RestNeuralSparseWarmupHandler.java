/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.apache.commons.lang.StringUtils;
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

import java.util.List;
import java.util.Locale;

import static org.opensearch.action.support.IndicesOptions.strictExpandOpen;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

/**
 * RestHandler for neural-sparse index warmup API. API provides the ability for a user to load specific indices' SEISMIC index
 * into memory.
 */
public class RestNeuralSparseWarmupHandler extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestNeuralSparseWarmupHandler.class);
    private static final String URL_PATH = "/warmup/{index}";
    public static String NAME = "neural_sparse_warmup_action";
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ClusterService clusterService;

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
        return ImmutableList.of(
            new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s%s", NeuralSearch.NEURAL_BASE_URI, URL_PATH))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        NeuralSparseWarmupRequest neuralSparseWarmupRequest = createNeuralSparseWarmupRequest(request);
        return channel -> client.execute(
            NeuralSparseWarmupAction.INSTANCE,
            neuralSparseWarmupRequest,
            new RestToXContentListener<>(channel)
        );
    }

    private NeuralSparseWarmupRequest createNeuralSparseWarmupRequest(RestRequest request) {
        String[] indexNames = StringUtils.split(request.param("index"), ",");
        Index[] indices = indexNameExpressionResolver.concreteIndices(clusterService.state(), strictExpandOpen(), indexNames);
        RestNeuralSparseClearCacheHandler.validateIndices(indices, clusterService, SPARSE_INDEX, "warm up cache");

        return new NeuralSparseWarmupRequest(indexNames);
    }
}

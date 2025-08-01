/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.neuralsearch.sparse.NeuralSparseIndexShard; // This to change
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.indices.IndicesService;
import org.opensearch.transport.TransportService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

/**
 * Transport Action for warming up neural-sparse indices. TransportBroadcastByNodeAction will distribute the request to
 * all shards across the cluster for the given indices. For each shard, shardOperation will be called and the
 * warmup will take place.
 */
public class NeuralSparseWarmupTransportAction extends TransportBroadcastByNodeAction<
    NeuralSparseWarmupRequest,
    NeuralSparseWarmupResponse,
    TransportBroadcastByNodeAction.EmptyResult> {
    public static Logger logger = LogManager.getLogger(NeuralSparseWarmupTransportAction.class);

    private IndicesService indicesService;

    @Inject
    public NeuralSparseWarmupTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            NeuralSparseWarmupAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            NeuralSparseWarmupRequest::new,
            ThreadPool.Names.SEARCH
        );
        this.indicesService = indicesService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected NeuralSparseWarmupResponse newResponse(
        NeuralSparseWarmupRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> emptyResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new NeuralSparseWarmupResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected NeuralSparseWarmupRequest readRequestFrom(StreamInput in) throws IOException {
        return new NeuralSparseWarmupRequest(in);
    }

    @Override
    protected EmptyResult shardOperation(NeuralSparseWarmupRequest request, ShardRouting shardRouting) throws IOException {
        NeuralSparseIndexShard neuralSparseIndexShard = new NeuralSparseIndexShard(
            indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id())
        );
        neuralSparseIndexShard.warmup();
        return EmptyResult.INSTANCE;
    }

    @Override
    protected ShardsIterator shards(ClusterState state, NeuralSparseWarmupRequest request, String[] concreteIndices) {
        return state.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, NeuralSparseWarmupRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, NeuralSparseWarmupRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}

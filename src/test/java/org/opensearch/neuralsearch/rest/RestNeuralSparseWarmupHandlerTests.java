/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import lombok.SneakyThrows;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.rest.RestChannel;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupAction;
import org.opensearch.neuralsearch.transport.NeuralSparseWarmupRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Locale;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

public class RestNeuralSparseWarmupHandlerTests extends OpenSearchTestCase {

    @Mock
    private ClusterService clusterService;

    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;

    @Mock
    private NodeClient nodeClient;

    @Mock
    private RestRequest restRequest;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Metadata metadata;

    @Mock
    private IndexMetadata indexMetadata;

    private RestNeuralSparseWarmupHandler handler;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        handler = new RestNeuralSparseWarmupHandler(clusterService, indexNameExpressionResolver);
    }

    public void testGetName() {
        assertEquals("neural_sparse_warmup_action", handler.getName());
    }

    public void testRoutes() {
        List<RestNeuralSparseWarmupHandler.Route> routes = handler.routes();
        assertEquals(1, routes.size());

        RestNeuralSparseWarmupHandler.Route route = routes.get(0);
        assertEquals(RestRequest.Method.POST, route.getMethod());
        assertEquals(String.format(Locale.ROOT, "%s/warmup/{index}", NeuralSearch.NEURAL_BASE_URI), route.getPath());
    }

    @SneakyThrows
    public void testPrepareRequestWithSingleIndex() {
        // Setup
        String indexName = "test-index";
        when(restRequest.param("index")).thenReturn(indexName);

        Index[] indices = { new Index(indexName, "uuid1") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { indexName }))).thenReturn(indices);

        setupValidSparseIndex(indexName);

        // Execute - use reflection to access the protected method
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
        verify(indexNameExpressionResolver).concreteIndices(any(), any(), eq(new String[] { indexName }));
    }

    @SneakyThrows
    public void testPrepareRequestWithMultipleIndices() {
        // Setup
        String indexNames = "index1,index2,index3";
        when(restRequest.param("index")).thenReturn(indexNames);

        Index[] indices = { new Index("index1", "uuid1"), new Index("index2", "uuid2"), new Index("index3", "uuid3") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { "index1", "index2", "index3" }))).thenReturn(
            indices
        );

        setupValidSparseIndices("index1", "index2", "index3");

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);

        // Verify
        assertNotNull(consumer);
        verify(indexNameExpressionResolver).concreteIndices(any(), any(), eq(new String[] { "index1", "index2", "index3" }));
    }

    public void testPrepareRequestWithInvalidSparseIndex() {
        // Setup
        String indexName = "invalid-index";
        when(restRequest.param("index")).thenReturn(indexName);

        Index[] indices = { new Index(indexName, "uuid1") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { indexName }))).thenReturn(indices);

        setupInvalidSparseIndex(indexName);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> handler.prepareRequest(restRequest, nodeClient)
        );

        assertTrue(exception.getMessage().contains(indexName));
        assertTrue(exception.getMessage().contains("neural_sparse_warmup_action"));
    }

    public void testPrepareRequestWithMixedValidInvalidIndices() {
        // Setup
        String indexNames = "valid-index,invalid-index";
        when(restRequest.param("index")).thenReturn(indexNames);

        Index[] indices = { new Index("valid-index", "uuid1"), new Index("invalid-index", "uuid2") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { "valid-index", "invalid-index" }))).thenReturn(
            indices
        );

        setupMixedSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> handler.prepareRequest(restRequest, nodeClient)
        );

        assertTrue(exception.getMessage().contains("[invalid-index]"));
        assertFalse(exception.getMessage().contains("[valid-index]"));
    }

    @SneakyThrows
    public void testCreateNeuralSparseWarmupRequest() {
        // Setup
        String indexNames = "index1,index2";
        when(restRequest.param("index")).thenReturn(indexNames);

        Index[] indices = { new Index("index1", "uuid1"), new Index("index2", "uuid2") };
        when(indexNameExpressionResolver.concreteIndices(any(), any(), eq(new String[] { "index1", "index2" }))).thenReturn(indices);

        setupValidSparseIndices("index1", "index2");

        // Setup nodeClient to capture the execute call
        doAnswer(invocation -> {
            assertEquals(NeuralSparseWarmupAction.INSTANCE, invocation.getArgument(0));
            NeuralSparseWarmupRequest request = invocation.getArgument(1);
            assertNotNull(request);
            return null;
        }).when(nodeClient).execute(any(), any(NeuralSparseWarmupRequest.class), any());

        // Execute
        Object consumer = handler.prepareRequest(restRequest, nodeClient);
        assertNotNull(consumer);

        // Execute the consumer to trigger the nodeClient.execute call
        ((CheckedConsumer<RestChannel, Exception>) consumer).accept(mock(RestChannel.class));

        // Verify the action was called
        verify(nodeClient).execute(eq(NeuralSparseWarmupAction.INSTANCE), any(NeuralSparseWarmupRequest.class), any());
    }

    private void setupValidSparseIndex(String indexName) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
    }

    private void setupValidSparseIndices(String... indexNames) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
    }

    private void setupInvalidSparseIndex(String indexName) {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());
    }

    private void setupMixedSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        IndexMetadata validIndexMetadata = mock(IndexMetadata.class);
        IndexMetadata invalidIndexMetadata = mock(IndexMetadata.class);

        when(validIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "true").build());
        when(invalidIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX, "false").build());

        when(metadata.getIndexSafe(any(Index.class))).thenAnswer(invocation -> {
            Index index = invocation.getArgument(0);
            if ("valid-index".equals(index.getName())) {
                return validIndexMetadata;
            } else {
                return invalidIndexMetadata;
            }
        });
    }
}

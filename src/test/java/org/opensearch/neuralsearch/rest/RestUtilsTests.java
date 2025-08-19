/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.neuralsearch.sparse.common.exception.NeuralSparseInvalidIndicesException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestUtilsTests extends OpenSearchTestCase {

    @Mock
    private ClusterService clusterService;

    @Mock
    private ClusterState clusterState;

    @Mock
    private Metadata metadata;

    @Mock
    private IndexMetadata indexMetadata;

    private static final String SPARSE_INDEX_SETTING = "index.sparse";
    private static final String API_OPERATION = "test_operation";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testValidateIndicesWithValidSparseIndices() {
        // Setup
        Index[] indices = { new Index("valid-index-1", "uuid1"), new Index("valid-index-2", "uuid2") };

        setupValidSparseIndices();

        // Execute - should not throw exception
        RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION);
    }

    public void testValidateIndicesWithInvalidSparseIndices() {
        // Setup
        Index[] indices = { new Index("invalid-index-1", "uuid1"), new Index("invalid-index-2", "uuid2") };

        setupInvalidSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        List<String> invalidIndices = exception.getInvalidIndices();
        assertEquals(2, invalidIndices.size());
        assertTrue(invalidIndices.contains("invalid-index-1"));
        assertTrue(invalidIndices.contains("invalid-index-2"));
        assertTrue(exception.getMessage().contains("test_operation"));
        assertTrue(exception.getMessage().contains("Request rejected"));
    }

    public void testValidateIndicesWithMixedValidInvalidIndices() {
        // Setup
        Index[] indices = { new Index("valid-index", "uuid1"), new Index("invalid-index", "uuid2") };

        setupMixedSparseIndices();

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        List<String> invalidIndices = exception.getInvalidIndices();
        assertEquals(1, invalidIndices.size());
        assertTrue(invalidIndices.contains("invalid-index"));
        assertFalse(invalidIndices.contains("valid-index"));
    }

    public void testValidateIndicesWithEmptyIndicesArray() {
        // Setup
        Index[] indices = {};

        // Execute - should not throw exception
        RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION);
    }

    public void testValidateIndicesWithNullClusterService() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, null, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithNullClusterState() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithNullMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithNullIndexMetadata() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithNullSettings() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(null);

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithFalseSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX_SETTING, "false").build());

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    public void testValidateIndicesWithMissingSparseIndexSetting() {
        // Setup
        Index[] indices = { new Index("test-index", "uuid1") };
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().build()); // No sparse setting

        // Execute & Verify
        NeuralSparseInvalidIndicesException exception = expectThrows(
            NeuralSparseInvalidIndicesException.class,
            () -> RestUtils.validateIndices(indices, clusterService, SPARSE_INDEX_SETTING, API_OPERATION)
        );

        assertEquals(1, exception.getInvalidIndices().size());
        assertTrue(exception.getInvalidIndices().contains("test-index"));
    }

    private void setupValidSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX_SETTING, "true").build());
    }

    private void setupInvalidSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.getIndexSafe(any(Index.class))).thenReturn(indexMetadata);
        when(indexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX_SETTING, "false").build());
    }

    private void setupMixedSparseIndices() {
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);

        IndexMetadata validIndexMetadata = mock(IndexMetadata.class);
        IndexMetadata invalidIndexMetadata = mock(IndexMetadata.class);

        when(validIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX_SETTING, "true").build());
        when(invalidIndexMetadata.getSettings()).thenReturn(Settings.builder().put(SPARSE_INDEX_SETTING, "false").build());

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

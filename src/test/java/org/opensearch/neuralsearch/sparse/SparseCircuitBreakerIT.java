/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparseCircuitBreakerIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    /**
     * Resets circuit breaker to default settings
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        updateClusterSettings(NeuralSearchSettings.NEURAL_CIRCUIT_BREAKER_LIMIT.getKey(), "50%");
        super.tearDown();
    }

    /**
     * Seismic features segment increases circuit breaker usage
     */
    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismic() {
        // Create Sparse Index
        int docCount = 100;
        Request request = configureSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original circuit breaker stats
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        // Ingest documents
        List<Map<String, Float>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
        }

        ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        // Verify circuit breaker stats increase after ingesting documents
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
    }

    /**
     * Seismic features segment increases circuit breaker usage
     */
    @SneakyThrows
    public void testMemoryStatsIncreaseWithSeismicAndMultiShard() {
        // Create Sparse Index
        int docCount = 100;
        int shardNumber = 3;
        Request request = configureSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount, shardNumber, 1);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Fetch original memory stats
        List<Long> originalCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        // Ingest documents
        List<Map<String, Float>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", randomFloat());
            tokens.put("2000", randomFloat());
            tokens.put("3000", randomFloat());
            tokens.put("4000", randomFloat());
            tokens.put("5000", randomFloat());
            docs.add(tokens);
        }

        for (int i = 0; i < shardNumber; ++i) {
            ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);
        }

        ingestDocuments(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        // Verify memory stats increase after ingesting documents
        List<Long> currentCircuitBreakerMemoryStats = getNeuralCircuitBreakerMemoryStatsAcrossNodes();

        long originalCircuitBreakerMemoryStatsSum = originalCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();
        long currentCircuitBreakerMemoryStatsSum = currentCircuitBreakerMemoryStats.stream().mapToLong(Long::longValue).sum();

        assertTrue(currentCircuitBreakerMemoryStatsSum > originalCircuitBreakerMemoryStatsSum);
    }
    // ingest index circuit breaker increase size

    // delete index circuit breaker release bytes

    // with circuit breaker limit, the query result is the same as before

}

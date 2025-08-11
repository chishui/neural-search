/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

/**
 * Integration tests for neural sparse cache operations (warm up and clear cache)
 */
public class NeuralSparseCacheIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX_NAME = "test-sparse-cache-index";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        createSparseIndex();
        indexTestDocuments();
        enableNeuralStats();
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification
     */
    public void testWarmUpCache() throws IOException {

        // Get initial memory usage
        double initialMemoryUsage = getSparseMemoryUsagePercentage();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response response = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        double afterWarmUpMemoryUsage = getSparseMemoryUsagePercentage();
        assertTrue("Memory usage should increase after warm up", afterWarmUpMemoryUsage >= initialMemoryUsage);
    }

    /**
     * Test clear cache API for sparse index with memory usage verification
     */
    public void testClearCache() throws IOException {

        // First warm up the cache
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        client().performRequest(warmUpRequest);

        // Get memory usage after warm up
        double afterWarmUpMemoryUsage = getSparseMemoryUsagePercentage();

        // Execute clear cache request
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response response = client().performRequest(clearCacheRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage decreased after clear cache
        double afterClearMemoryUsage = getSparseMemoryUsagePercentage();
        assertTrue("Memory usage should decrease after clear cache", afterClearMemoryUsage <= afterWarmUpMemoryUsage);
    }

    /**
     * Test warm up cache API with non-sparse index should fail
     */
    public void testWarmUpCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index";

        // Create non-sparse index
        try {
            Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
            createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
            client().performRequest(createIndexRequest);

            // Try to warm up cache - should fail
            Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + nonSparseIndex);
            expectThrows(IOException.class, () -> client().performRequest(warmUpRequest));
        } catch (IOException e) {
            // Expected behavior
        }
    }

    /**
     * Test clear cache API with non-sparse index should fail
     */
    public void testClearCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index-2";

        // Create non-sparse index
        try {
            Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
            createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
            client().performRequest(createIndexRequest);

            // Try to clear cache - should fail
            Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + nonSparseIndex);
            expectThrows(IOException.class, () -> client().performRequest(clearCacheRequest));
        } catch (IOException e) {
            // Expected behavior
        }
    }

    /**
     * Test warm up and clear cache operations in sequence with memory usage verification
     */
    public void testWarmUpAndClearCacheSequence() throws IOException {

        // Get initial memory usage
        double initialMemoryUsage = getSparseMemoryUsagePercentage();

        // Warm up cache
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify memory usage increased
        double afterFirstWarmUpMemoryUsage = getSparseMemoryUsagePercentage();
        assertTrue("Memory usage should increase after first warm up", afterFirstWarmUpMemoryUsage >= initialMemoryUsage);

        // Clear cache
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(clearCacheResponse.getStatusLine().getStatusCode()));

        // Verify memory usage decreased
        double afterClearMemoryUsage = getSparseMemoryUsagePercentage();
        assertTrue("Memory usage should decrease after clear cache", afterClearMemoryUsage <= afterFirstWarmUpMemoryUsage);

        // Warm up again
        Response secondWarmUpResponse = client().performRequest(warmUpRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(secondWarmUpResponse.getStatusLine().getStatusCode()));

        // Verify memory usage increased again
        double afterSecondWarmUpMemoryUsage = getSparseMemoryUsagePercentage();
        assertTrue("Memory usage should increase after second warm up", afterSecondWarmUpMemoryUsage >= afterClearMemoryUsage);
    }

    private void createSparseIndex() throws IOException {
        String indexSettings = """
            {
                "settings": {
                    "index": {
                        "sparse": true,
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                },
                "mappings": {
                    "properties": {
                        "%s": {
                            "type": "sparse_tokens",
                            "method": {
                                "name": "seismic",
                                "parameters": {
                                    "n_postings": 100,
                                    "summary_prune_ratio": 0.4,
                                    "cluster_ratio": 0.1,
                                    "approximate_threshold": 8
                                }
                            }
                        }
                    }
                }
            }
            """.formatted(TEST_SPARSE_FIELD_NAME);

        Request request = new Request("PUT", "/" + TEST_INDEX_NAME);
        request.setJsonEntity(indexSettings);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private void indexTestDocuments() throws IOException {
        // Index a few test documents with sparse tokens
        for (int i = 1; i <= 3; i++) {
            Map<String, Float> sparseTokens = createRandomTokenWeightMap(
                java.util.List.of("token1", "token2", "token3", "token4", "token5")
            );

            String docJson = String.format("""
                {
                    "%s": %s
                }
                """, TEST_SPARSE_FIELD_NAME, convertTokensToJson(sparseTokens));

            Request indexRequest = new Request("POST", "/" + TEST_INDEX_NAME + "/_doc/" + i + "?refresh=true");
            indexRequest.setJsonEntity(docJson);
            Response response = client().performRequest(indexRequest);
            assertEquals(RestStatus.CREATED, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        }
    }

    private String convertTokensToJson(Map<String, Float> tokens) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Float> entry : tokens.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"").append(entry.getKey()).append("\":").append(entry.getValue());
            first = false;
        }
        json.append("}");
        return json.toString();
    }

    /**
     * Enable neural search stats
     */
    private void enableNeuralStats() throws IOException {
        Request enableStatsRequest = new Request("PUT", "/_cluster/settings");
        enableStatsRequest.setJsonEntity("""
            {
                "persistent": {
                    "plugins.neural_search.stats_enabled": "true"
                }
            }
            """);
        client().performRequest(enableStatsRequest);
    }

    /**
     * Get sparse memory usage percentage from neural stats
     */
    private double getSparseMemoryUsagePercentage() throws IOException {
        Request statsRequest = new Request("GET", "/_plugins/_neural/stats");
        Response response = client().performRequest(statsRequest);

        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            response.getEntity().getContent()
        ).map();

        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) responseMap.get("nodes");

        // Get first node's memory stats
        for (Object nodeStats : nodes.values()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> nodeStatsMap = (Map<String, Object>) nodeStats;
            @SuppressWarnings("unchecked")
            Map<String, Object> memory = (Map<String, Object>) nodeStatsMap.get("memory");
            @SuppressWarnings("unchecked")
            Map<String, Object> sparse = (Map<String, Object>) memory.get("sparse");

            String percentageStr = (String) sparse.get("sparse_memory_usage_percentage");
            return Double.parseDouble(percentageStr.replace("%", ""));
        }

        return 0.0;
    }
}

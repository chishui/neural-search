/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.plugin.NeuralSearch;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.stats.metrics.MetricStatName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Integration tests for neural sparse cache operations (warm up and clear cache)
 */
public class NeuralSparseCacheOperationIT extends SparseBaseIT {

    private static final String TEST_INDEX_NAME = "test-sparse-cache-index";
    private static final String TEST_TEXT_FIELD_NAME = "text";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final String SPARSE_MEMORY_USAGE_METRIC_NAME = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getNameString();
    private static final String SPARSE_MEMORY_USAGE_METRIC_PATH = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE.getFullPath();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        int docCount = 100;
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.5f, docCount);
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
        ingestDocumentsAndForceMerge(TEST_INDEX_NAME, TEST_TEXT_FIELD_NAME, TEST_SPARSE_FIELD_NAME, docs);
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), true);
    }

    /**
     * Resets circuit breaker and neural stats to default settings
     */
    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        updateClusterSettings(NeuralSearchSettings.NEURAL_STATS_ENABLED.getKey(), false);
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        super.tearDown();
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testWarmUpCache() {
        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        long[] afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterWarmUpSparseMemoryUsageSum = Arrays.stream(afterWarmUpSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
    }

    /**
     * Test clear cache API for sparse index with memory usage verification
     */
    @SneakyThrows
    public void testClearCache() {
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

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
        long[] afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterClearCacheSparseMemoryUsageSum = Arrays.stream(afterClearCacheSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testWarmUpMultiShardReplicasCache() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMultiShardReplicasIndex();

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        long[] afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterWarmUpSparseMemoryUsageSum = Arrays.stream(afterWarmUpSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Multiple shards and replicas.
     */
    @SneakyThrows
    public void testClearMultiShardReplicasCache() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMultiShardReplicasIndex();
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

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
        long[] afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterClearCacheSparseMemoryUsageSum = Arrays.stream(afterClearCacheSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
    }

    /**
     * Test warm up cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testWarmUpCache_MixSeismicAndRankFeatures() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex();

        // First clear cache before warm up
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + TEST_INDEX_NAME);
        Response clearCacheResponse = client().performRequest(clearCacheRequest);
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

        // Execute warm up cache request
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + TEST_INDEX_NAME);
        Response warmUpResponse = client().performRequest(warmUpRequest);

        assertEquals(RestStatus.OK, RestStatus.fromCode(warmUpResponse.getStatusLine().getStatusCode()));

        // Verify response structure
        Map<String, Object> responseMap = createParser(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            warmUpResponse.getEntity().getContent()
        ).map();

        assertNotNull(responseMap);
        assertTrue(responseMap.containsKey("_shards"));

        // Verify memory usage increased after warm up
        long[] afterWarmUpSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterWarmUpSparseMemoryUsageSum = Arrays.stream(afterWarmUpSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should increase after warm up", afterWarmUpSparseMemoryUsageSum > originalSparseMemoryUsageSum);
    }

    /**
     * Test clear cache API for sparse index with memory usage verification. Mix SEISMIC and Rank Feature.
     */
    @SneakyThrows
    public void testClearCache_MixSeismicAndRankFeatures() {
        Request request = new Request("DELETE", "/" + TEST_INDEX_NAME);
        client().performRequest(request);
        // Create Sparse Index
        prepareMixSeismicRankFeaturesIndex();
        long[] originalSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long originalSparseMemoryUsageSum = Arrays.stream(originalSparseMemoryUsageStats).sum();

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
        long[] afterClearCacheSparseMemoryUsageStats = getSparseMemoryUsageStatsAcrossNodes();
        long afterClearCacheSparseMemoryUsageSum = Arrays.stream(afterClearCacheSparseMemoryUsageStats).sum();
        assertTrue("Memory usage should decrease after clear cache", afterClearCacheSparseMemoryUsageSum < originalSparseMemoryUsageSum);
    }

    /**
     * Test warm up cache API with non-sparse index should fail
     */
    @SneakyThrows
    public void testWarmUpCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index";

        Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
        createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        client().performRequest(createIndexRequest);

        // Try to warm up cache - should fail
        Request warmUpRequest = new Request("POST", "/_plugins/_neural/warmup/" + nonSparseIndex);
        expectThrows(IOException.class, () -> client().performRequest(warmUpRequest));

    }

    /**
     * Test clear cache API with non-sparse index should fail
     */
    @SneakyThrows
    public void testClearCacheWithNonSparseIndex() {
        String nonSparseIndex = "non-sparse-index-2";

        Request createIndexRequest = new Request("PUT", "/" + nonSparseIndex);
        createIndexRequest.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        client().performRequest(createIndexRequest);

        // Try to clear cache - should fail
        Request clearCacheRequest = new Request("POST", "/_plugins/_neural/clear_cache/" + nonSparseIndex);
        expectThrows(IOException.class, () -> client().performRequest(clearCacheRequest));

    }

    @SneakyThrows
    private long[] getSparseMemoryUsageStatsAcrossNodes() {
        Request request = new Request("GET", NeuralSearch.NEURAL_BASE_URI + "/stats/" + SPARSE_MEMORY_USAGE_METRIC_NAME);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> nodeStatsResponseList = parseNodeStatsResponse(responseBody);

        List<Long> sparseMemoryUsageStats = new ArrayList<>();
        for (Map<String, Object> nodeStatsResponse : nodeStatsResponseList) {
            // we do not use breakers.neural_search.estimated_size_in_bytes due to precision limitation by memory stats
            String stringValue = getNestedValue(nodeStatsResponse, SPARSE_MEMORY_USAGE_METRIC_PATH).toString();
            sparseMemoryUsageStats.add(parseFractionalSize(stringValue));
        }
        return sparseMemoryUsageStats.stream().mapToLong(Long::longValue).toArray();
    }

    private static long parseFractionalSize(String value) {
        value = value.trim().toLowerCase(Locale.ROOT);
        double number;
        long multiplier;

        if (value.endsWith("kb")) {
            number = Double.parseDouble(value.replace("kb", "").trim());
            multiplier = 1024L;
        } else if (value.endsWith("mb")) {
            number = Double.parseDouble(value.replace("mb", "").trim());
            multiplier = 1024L * 1024L;
        } else if (value.endsWith("gb")) {
            number = Double.parseDouble(value.replace("gb", "").trim());
            multiplier = 1024L * 1024L * 1024L;
        } else if (value.endsWith("b")) {
            number = Double.parseDouble(value.replace("b", "").trim());
            multiplier = 1L;
        } else {
            throw new IllegalArgumentException("Unknown size unit: " + value);
        }

        return Math.round(number * multiplier);
    }

    @SneakyThrows
    private void prepareMultiShardReplicasIndex() {
        int shards = 3;
        int docCount = 100;
        // effective number of replica is capped by the number of OpenSearch nodes minus 1
        int replicas = Math.min(3, getNodeCount() - 1);
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 100, 0.4f, 0.1f, docCount, shards, replicas);
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

        List<String> routingIds = generateUniqueRoutingIds(shards);
        for (int i = 0; i < shards; ++i) {
            ingestDocuments(
                TEST_INDEX_NAME,
                TEST_TEXT_FIELD_NAME,
                TEST_SPARSE_FIELD_NAME,
                docs,
                Collections.emptyList(),
                i * docCount + 1,
                routingIds.get(i)
            );
        }

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME, shards, replicas);
    }

    @SneakyThrows
    private void prepareMixSeismicRankFeaturesIndex() {
        createSparseIndex(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, 4, 0.4f, 0.5f, 4);

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            null,
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_TEXT_FIELD_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            null,
            4
        );
    }
}

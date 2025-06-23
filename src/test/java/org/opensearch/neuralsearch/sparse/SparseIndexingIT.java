/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.SparseSettings.ALGO_TRIGGER_THRESHOLD_SETTING;
import static org.opensearch.neuralsearch.sparse.SparseSettings.IS_SPARSE_INDEX_SETTING;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

/**
 * Integration tests for sparse index feature
 */
public class SparseIndexingIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final List<String> TEST_TOKENS = List.of("hello", "world", "test", "sparse", "index");
    private static final String ALGO_NAME = "seismic";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    private String prepareIndexSettings(int triggerDocCount) throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("index")
            .field("sparse", true)
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
            .field("sparse.algo_trigger_doc_count", triggerDocCount)
            .endObject()
            .endObject();
        return settingBuilder.toString();
    }

    private String prepareIndexMapping(int nPostings, float alpha, float clusterRatio) throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME) // Integer: length of posting list
            .startObject("parameters")
            .field("n_postings", nPostings) // Integer: length of posting list
            .field("summary_prune_ratio", alpha) // Float
            .field("cluster_ratio", clusterRatio) // Float: cluster ratio
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder.toString();
    }

    /**
     * Test creating an index with sparse index setting enabled
     */
    public void testCreateSparseIndex() throws IOException {
        String indexSettings = prepareIndexSettings(8);
        String indexMappings = prepareIndexMapping(100, 0.4f, 0.1f);

        Request request = new Request("PUT", "/" + TEST_INDEX_NAME);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        System.out.println(body);
        request.setJsonEntity(body);

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        assertEquals("true", indexSettingsMap.get("sparse"));
    }

    /**
     * Test indexing documents with sparse tokens field
     */
    public void testIndexDocumentsWithSparseTokensField() throws IOException {
        // Create index with sparse index setting enabled
        testCreateSparseIndex();

        // Create a document with sparse tokens field
        Map<String, Float> sparseTokens = createRandomTokenWeightMap(TEST_TOKENS);

        // Index the document
        addSparseEncodingDoc(TEST_INDEX_NAME, "1", List.of(TEST_SPARSE_FIELD_NAME), List.of(sparseTokens));

        // Verify document was indexed
        assertEquals(1, getDocCount(TEST_INDEX_NAME));

        // Get the document and verify its content
        Map<String, Object> document = getDocById(TEST_INDEX_NAME, "1");
        assertNotNull(document);

        Map<String, Object> source = (Map<String, Object>) document.get("_source");
        assertNotNull(source);

        Map<String, Object> sparseField = (Map<String, Object>) source.get(TEST_SPARSE_FIELD_NAME);
        assertNotNull(sparseField);

        // Verify the sparse tokens are present
        for (String token : TEST_TOKENS) {
            if (sparseTokens.containsKey(token)) {
                assertTrue(sparseField.containsKey(token));
                assertEquals(sparseTokens.get(token).doubleValue(), ((Number) sparseField.get(token)).doubleValue(), 0.001);
            }
        }
    }

    /**
     * Test creating an index with sparse index setting disabled (default)
     */
    public void testCreateNonSparseIndex() throws IOException {
        // Create index without sparse index setting (default is false)
        Settings indexSettings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("lambda", 100) // Integer: length of posting list
            .field("alpha", 0.5) // Float
            .field("beta", 5) // Integer: number of clusters
            .field("cluster_until_doc_count_reach", 1000)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        Request request = new Request("PUT", "/" + TEST_INDEX_NAME + "_non_sparse");
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\n"
                    + "  \"settings\": {\n"
                    + "    \"index.number_of_shards\": 1,\n"
                    + "    \"index.number_of_replicas\": 0\n"
                    + "  },\n"
                    + "  \"mappings\": %s\n"
                    + "}",
                mappingBuilder.toString()
            )
        );

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(TEST_INDEX_NAME + "_non_sparse"));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + TEST_INDEX_NAME + "_non_sparse" + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(TEST_INDEX_NAME + "_non_sparse");
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        // Sparse setting should not be present (default is false)
        assertFalse(indexSettingsMap.containsKey("sparse"));
    }

    /**
     * Test updating the sparse memory usage setting
     */
    public void testUpdateSparseMemoryUsageSetting() throws IOException {
        // Create index with sparse index setting enabled
        testCreateSparseIndex();

        // Index some documents to populate sparse data structures
        for (int i = 0; i < 5; i++) {
            Map<String, Float> sparseTokens = createRandomTokenWeightMap(TEST_TOKENS);
            addSparseEncodingDoc(TEST_INDEX_NAME, String.valueOf(i), List.of(TEST_SPARSE_FIELD_NAME), List.of(sparseTokens));
        }

        // Update the sparse memory usage setting
        Request updateSettingsRequest = new Request("PUT", "/" + TEST_INDEX_NAME + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" + "  \"index\": {\n" + "    \"sparse.memory\": true\n" + "  }\n" + "}");

        Response updateSettingsResponse = client().performRequest(updateSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(updateSettingsResponse.getStatusLine().getStatusCode()));

        // Verify the setting was updated
        Request getSettingsRequest = new Request("GET", "/" + TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        assertEquals("true", indexSettingsMap.get("sparse.memory"));
    }

    /**
     * Test that sparse index setting cannot be updated after index creation (it's a final setting)
     */
    public void testCannotUpdateSparseIndexSetting() throws IOException {
        // Create index without sparse index setting (default is false)
        testCreateNonSparseIndex();

        // Try to update the sparse index setting (should fail because it's final)
        Request updateSettingsRequest = new Request("PUT", "/" + TEST_INDEX_NAME + "_non_sparse" + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" + "  \"index\": {\n" + "    \"sparse\": true\n" + "  }\n" + "}");

        // This should throw an exception because sparse is a final setting
        expectThrows(IOException.class, () -> { client().performRequest(updateSettingsRequest); });
    }

    /**
     * Test error handling when creating a sparse tokens field with invalid parameters
     */
    public void testSparseTokensFieldWithInvalidParameters() throws IOException {
        // Create index with sparse index setting enabled
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IS_SPARSE_INDEX_SETTING.getKey(), true)
            .put(ALGO_TRIGGER_THRESHOLD_SETTING.getKey(), 1000)
            .build();

        // Try to create a mapping with negative lambda (should fail)
        XContentBuilder invalidMappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("lambda", -10) // Invalid negative integer value
            .field("alpha", 0.5) // Float
            .field("beta", 5) // Integer: number of clusters
            .field("cluster_until_doc_count_reach", 1000)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String indexName = TEST_INDEX_NAME + "_invalid_params";
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\n"
                    + "  \"settings\": {\n"
                    + "    \"index.number_of_shards\": 1,\n"
                    + "    \"index.number_of_replicas\": 0,\n"
                    + "    \"index.sparse\": true\n"
                    + "  },\n"
                    + "  \"mappings\": %s\n"
                    + "}",
                invalidMappingBuilder.toString()
            )
        );

        // This should throw an exception because of invalid parameters
        expectThrows(IOException.class, () -> { client().performRequest(request); });
    }

    /**
     * Test creating sparse tokens field with different method parameters
     */
    public void testSparseTokensFieldWithDifferentMethodParameters() throws IOException {
        // Create index with sparse index setting enabled
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IS_SPARSE_INDEX_SETTING.getKey(), true)
            .put(ALGO_TRIGGER_THRESHOLD_SETTING.getKey(), 1000)
            .build();

        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("sparse_field_default")
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            // Using default values
            .field("lambda", 100) // Integer: length of posting list
            .field("alpha", 0.5) // Float
            .field("beta", 5) // Integer: number of clusters
            .field("cluster_until_doc_count_reach", 1000)
            .endObject()
            .endObject()
            .startObject("sparse_field_custom")
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            // Using custom values
            .field("lambda", 200) // Integer: length of posting list
            .field("alpha", 0.7) // Float
            .field("beta", 10) // Integer: number of clusters
            .field("cluster_until_doc_count_reach", 500)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String indexName = TEST_INDEX_NAME + "_method_params";
        Request request = new Request("PUT", "/" + indexName);
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                "{\n"
                    + "  \"settings\": {\n"
                    + "    \"index.number_of_shards\": 1,\n"
                    + "    \"index.number_of_replicas\": 0,\n"
                    + "    \"index.sparse\": true\n"
                    + "  },\n"
                    + "  \"mappings\": %s\n"
                    + "}",
                mappingBuilder.toString()
            )
        );

        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(indexName));

        // Get the mapping and verify the method parameters
        Request getMappingRequest = new Request("GET", "/" + indexName + "/_mapping");
        Response getMappingResponse = client().performRequest(getMappingRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getMappingResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getMappingResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(indexName);
        Map<String, Object> mappingsMap = (Map<String, Object>) indexMap.get("mappings");
        Map<String, Object> propertiesMap = (Map<String, Object>) mappingsMap.get("properties");

        // Verify default field parameters
        Map<String, Object> defaultFieldMap = (Map<String, Object>) propertiesMap.get("sparse_field_default");
        Map<String, Object> defaultMethodMap = (Map<String, Object>) defaultFieldMap.get("method");
        assertEquals(100, ((Number) defaultMethodMap.get("lambda")).intValue());
        assertEquals(0.5, ((Number) defaultMethodMap.get("alpha")).doubleValue(), 0.001);
        assertEquals(5, ((Number) defaultMethodMap.get("beta")).intValue());
        assertEquals(1000, ((Number) defaultMethodMap.get("cluster_until_doc_count_reach")).intValue());

        // Verify custom field parameters
        Map<String, Object> customFieldMap = (Map<String, Object>) propertiesMap.get("sparse_field_custom");
        Map<String, Object> customMethodMap = (Map<String, Object>) customFieldMap.get("method");
        assertEquals(200, ((Number) customMethodMap.get("lambda")).intValue());
        assertEquals(0.7, ((Number) customMethodMap.get("alpha")).doubleValue(), 0.001);
        assertEquals(10, ((Number) customMethodMap.get("beta")).intValue());
        assertEquals(500, ((Number) customMethodMap.get("cluster_until_doc_count_reach")).intValue());

        // Index documents with both fields
        Map<String, Float> sparseTokens1 = createRandomTokenWeightMap(TEST_TOKENS);
        Map<String, Float> sparseTokens2 = createRandomTokenWeightMap(TEST_TOKENS);

        // Create a document with both sparse fields
        Request indexRequest = new Request("POST", "/" + indexName + "/_doc/1?refresh=true");
        XContentBuilder docBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("sparse_field_default", sparseTokens1)
            .field("sparse_field_custom", sparseTokens2)
            .endObject();

        indexRequest.setJsonEntity(docBuilder.toString());
        Response indexResponse = client().performRequest(indexRequest);
        assertEquals(RestStatus.CREATED, RestStatus.fromCode(indexResponse.getStatusLine().getStatusCode()));

        // Verify document was indexed
        assertEquals(1, getDocCount(indexName));
    }

    /**
     * Test searching on a sparse index
     */
    public void testSearchOnSparseIndex() throws Exception {
        // Create index with sparse index setting enabled and prepare model
        testCreateSparseIndex();
        String modelId = prepareSparseEncodingModel();

        // Index multiple documents with different sparse tokens
        for (int i = 0; i < 5; i++) {
            Map<String, Float> sparseTokens = new HashMap<>();
            // Create documents with different token weights
            for (String token : TEST_TOKENS) {
                sparseTokens.put(token, (float) (i + 1) * 0.1f);
            }

            addSparseEncodingDoc(TEST_INDEX_NAME, String.valueOf(i), List.of(TEST_SPARSE_FIELD_NAME), List.of(sparseTokens));
        }

        // Refresh the index
        Request refreshRequest = new Request("POST", "/" + TEST_INDEX_NAME + "/_refresh");
        client().performRequest(refreshRequest);

        // Verify all documents were indexed
        assertEquals(5, getDocCount(TEST_INDEX_NAME));

        // Create a neural sparse query
        String queryText = "hello world test";
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().fieldName(TEST_SPARSE_FIELD_NAME)
            .queryText(queryText)
            .modelId(modelId);

        // Execute the search
        Map<String, Object> searchResponseAsMap = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 5);

        // Verify search results
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");

        // Verify we got results
        assertFalse(hitsList.isEmpty());

        // Verify results have scores
        for (Map<String, Object> hit : hitsList) {
            assertTrue(hit.containsKey("_score"));
            assertTrue(((Number) hit.get("_score")).floatValue() > 0);
        }

        // Test with a boolean query combining multiple conditions
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(neuralSparseQueryBuilder);

        // Execute the search with the boolean query
        Map<String, Object> boolSearchResponseAsMap = search(TEST_INDEX_NAME, boolQueryBuilder, 5);

        // Verify search results
        Map<String, Object> boolHitsMap = (Map<String, Object>) boolSearchResponseAsMap.get("hits");
        List<Map<String, Object>> boolHitsList = (List<Map<String, Object>>) boolHitsMap.get("hits");

        // Verify we got results
        assertFalse(boolHitsList.isEmpty());
    }
}

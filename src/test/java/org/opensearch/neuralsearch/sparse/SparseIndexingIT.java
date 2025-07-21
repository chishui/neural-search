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
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.createRandomTokenWeightMap;

/**
 * Integration tests for sparse index feature
 */
public class SparseIndexingIT extends BaseNeuralSearchIT {

    private static final String TEST_INDEX_NAME = "test-sparse-index";
    private static final String NON_SPARSE_TEST_INDEX_NAME = TEST_INDEX_NAME + "_non_sparse";
    private static final String INVALID_PARAM_TEST_INDEX_NAME = TEST_INDEX_NAME + "_invalid";
    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";
    private static final List<String> TEST_TOKENS = List.of("hello", "world", "test", "sparse", "index");
    private static final String ALGO_NAME = "seismic";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Test creating an index with sparse index setting enabled
     */
    public void testCreateSparseIndex() throws IOException {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 100, 0.4f, 0.1f, 8);
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
        String indexMappings = prepareIndexMapping(100, 0.4f, 0.1f, 8);

        Request request = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // Verify index exists
        assertTrue(indexExists(NON_SPARSE_TEST_INDEX_NAME));

        // Verify index settings
        Request getSettingsRequest = new Request("GET", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        Response getSettingsResponse = client().performRequest(getSettingsRequest);
        assertEquals(RestStatus.OK, RestStatus.fromCode(getSettingsResponse.getStatusLine().getStatusCode()));

        Map<String, Object> responseMap = createParser(XContentType.JSON.xContent(), getSettingsResponse.getEntity().getContent()).map();

        Map<String, Object> indexMap = (Map<String, Object>) responseMap.get(NON_SPARSE_TEST_INDEX_NAME);
        Map<String, Object> settingsMap = (Map<String, Object>) indexMap.get("settings");
        Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsMap.get("index");

        // Sparse setting should not be present (default is false)
        assertFalse(indexSettingsMap.containsKey("sparse"));
    }

    /**
     * Test that sparse index setting cannot be updated after index creation (it's a final setting)
     */
    public void testCannotUpdateSparseIndexSetting() throws IOException {
        // Create index without sparse index setting (default is false)
        testCreateNonSparseIndex();

        // Try to update the sparse index setting (should fail because it's final)
        Request updateSettingsRequest = new Request("PUT", "/" + NON_SPARSE_TEST_INDEX_NAME + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" + "  \"index\": {\n" + "    \"sparse\": true\n" + "  }\n" + "}");

        // This should throw an exception because sparse is a final setting
        expectThrows(IOException.class, () -> { client().performRequest(updateSettingsRequest); });
    }

    /**
     * Test error handling when creating a sparse tokens field with invalid parameters
     */
    public void testSparseTokensFieldWithInvalidParameters() throws IOException {
        expectThrows(
            IOException.class,
            () -> { client().performRequest(configureSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, -1, 0.4f, 0.1f, 8)); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters2() throws IOException {
        expectThrows(
            IOException.class,
            () -> { client().performRequest(configureSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, 100, -0.4f, 0.1f, 8)); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters3() throws IOException {
        expectThrows(
            IOException.class,
            () -> { client().performRequest(configureSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, 100, 0.4f, -0.1f, 8)); }
        );
    }

    public void testSparseTokensFieldWithInvalidParameters4() throws IOException {
        expectThrows(
            IOException.class,
            () -> { client().performRequest(configureSparseIndex(INVALID_PARAM_TEST_INDEX_NAME, 100, 0.4f, 0.1f, -8)); }
        );
    }

    /**
     * Test creating sparse tokens field with different method parameters
     */
    public void testSparseTokensFieldWithAdditionParameters() throws IOException {
        // Create index with sparse index setting enabled
        String indexSettings = prepareIndexSettings();
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(TEST_SPARSE_FIELD_NAME)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME) // Integer: length of posting list
            .startObject("parameters")
            .field("n_postings", 100) // Integer: length of posting list
            .field("summary_prune_ratio", 0.1f) // Float
            .field("cluster_ratio", 0.1f) // Float: cluster ratio
            .field("algo_trigger_doc_count", 8)
            .field("additional_parameter", 8)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        String indexName = TEST_INDEX_NAME + "_method_params";
        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            mappingBuilder.toString()
        );
        request.setJsonEntity(body);
        expectThrows(IOException.class, () -> client().performRequest(request));
    }

    private String prepareIndexSettings() throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("index")
            .field("sparse", true)
            .field("number_of_shards", 1)
            .field("number_of_replicas", 0)
            .endObject()
            .endObject();
        return settingBuilder.toString();
    }

    private String prepareIndexMapping(int nPostings, float alpha, float clusterRatio, int docTriggerPoint) throws IOException {
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
            .field("algo_trigger_doc_count", docTriggerPoint)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder.toString();
    }

    private Request configureSparseIndex(String indexName, int nPostings, float alpha, float clusterRatio, int docTriggerPoint)
        throws IOException {
        String indexSettings = prepareIndexSettings();
        String indexMappings = prepareIndexMapping(nPostings, alpha, clusterRatio, docTriggerPoint);
        Request request = new Request("PUT", "/" + indexName);
        String body = String.format(
            Locale.ROOT,
            "{\n" + "  \"settings\": %s,\n" + "  \"mappings\": %s\n" + "}",
            indexSettings,
            indexMappings
        );
        request.setJsonEntity(body);
        return request;
    }
}

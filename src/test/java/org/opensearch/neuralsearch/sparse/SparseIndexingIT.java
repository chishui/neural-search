/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
    private static final String TEXT_FIELD = "text";

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

    public void testIngestDocumentsAllSeismicPostingPruning() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 4, 0.4f, 0.5f, 8);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            )
        );

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("8", "7", "6", "5"), actualIds);
    }

    public void testIngestDocumentsMixSeismicWithRankFeatures() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 4, 0.4f, 0.5f, 4);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            4
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(7, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("8", "7", "6", "5", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsWithAllRankFeatures() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 4, 0.4f, 0.5f, 100);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.1f, "2000", 0.1f), Map.of("1000", 0.2f, "2000", 0.2f), Map.of("1000", 0.3f, "2000", 0.3f)),
            1
        );
        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.4f, "2000", 0.4f), Map.of("1000", 0.5f, "2000", 0.5f)),
            4
        );

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(Map.of("1000", 0.6f, "2000", 0.6f), Map.of("1000", 0.7f, "2000", 0.7f), Map.of("1000", 0.8f, "2000", 0.8f)),
            6
        );

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(8, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("8", "7", "6", "5", "4", "3", "2", "1"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithCut() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 4, 0.4f, 0.5f, 8);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f),
                Map.of("3000", 0.0001f)
            )
        );

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f, "3000", 64.0f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(1, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("9"), actualIds);
    }

    public void testIngestDocumentsSeismicHeapFactor() throws Exception {
        final int docCount = 100;
        Request request = configureSparseIndex(TEST_INDEX_NAME, docCount, 0.4f, 0.5f, docCount);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        List<Map<String, Float>> docs = new ArrayList<>();
        for (int i = 0; i < docCount; ++i) {
            Map<String, Float> tokens = new HashMap<>();
            tokens.put("1000", (float) Math.random());
            tokens.put("2000", (float) Math.random());
            tokens.put("3000", (float) Math.random());
            tokens.put("4000", (float) Math.random());
            tokens.put("5000", (float) Math.random());
            docs.add(tokens);
        }

        ingestDocuments(TEST_INDEX_NAME, TEST_SPARSE_FIELD_NAME, docs);

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            0.000001f,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertTrue(getHitCount(searchResults) < docCount);

        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            1,
            100000,
            docCount,
            Map.of("1000", 0.12f, "2000", 0.64f, "3000", 0.87f, "4000", 0.53f)
        );
        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, docCount);
        assertNotNull(searchResults);
        assertEquals(docCount, getHitCount(searchResults));
    }

    public void testIngestDocumentsAllSeismicWithPreFiltering() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 8, 0.4f, 0.5f, 8);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "tree", "apple", "tree", "apple", "tree", "apple", "tree"),
            1
        );

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEXT_FIELD, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("1", "3", "5", "7"), actualIds);
        // filter tree
        filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEXT_FIELD, "tree"));
        neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            10,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(4, getHitCount(searchResults));
        actualIds = getDocIDs(searchResults);
        assertEquals(Set.of("2", "4", "6", "8"), actualIds);
    }

    public void testIngestDocumentsAllSeismicWithPostFiltering() throws Exception {
        Request request = configureSparseIndex(TEST_INDEX_NAME, 8, 0.4f, 0.5f, 8);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        ingestDocuments(
            TEST_INDEX_NAME,
            TEST_SPARSE_FIELD_NAME,
            List.of(
                Map.of("1000", 0.1f, "2000", 0.1f),
                Map.of("1000", 0.2f, "2000", 0.2f),
                Map.of("1000", 0.3f, "2000", 0.3f),
                Map.of("1000", 0.4f, "2000", 0.4f),
                Map.of("1000", 0.5f, "2000", 0.5f),
                Map.of("1000", 0.6f, "2000", 0.6f),
                Map.of("1000", 0.7f, "2000", 0.7f),
                Map.of("1000", 0.8f, "2000", 0.8f)
            ),
            List.of("apple", "apple", "apple", "apple", "apple", "apple", "apple", "tree"),
            1
        );

        forceMerge(TEST_INDEX_NAME);
        // wait until force merge complete
        waitForSegmentMerge(TEST_INDEX_NAME);

        // filter apple
        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(new MatchQueryBuilder(TEXT_FIELD, "apple"));

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = getNeuralSparseQueryBuilder(
            TEST_SPARSE_FIELD_NAME,
            2,
            1.0f,
            4,
            Map.of("1000", 0.1f, "2000", 0.2f),
            filter
        );

        Map<String, Object> searchResults = search(TEST_INDEX_NAME, neuralSparseQueryBuilder, 10);
        assertNotNull(searchResults);
        assertEquals(3, getHitCount(searchResults));
        Set<String> actualIds = getDocIDs(searchResults);
        // results with k = 4 are 5, 6, 7, 8, filter results are 1, 2, 3, 4, 5, 6, 7
        // intersection of both are 5, 6, 7
        assertEquals(Set.of("5", "6", "7"), actualIds);
    }

    private void ingestDocuments(String index, String field, List<Map<String, Float>> docTokens) {
        ingestDocuments(index, field, docTokens, 1);
    }

    private void ingestDocuments(String index, String field, List<Map<String, Float>> docTokens, int startingId) {
        StringBuilder payloadBuilder = new StringBuilder();
        for (int i = 0; i < docTokens.size(); i++) {
            Map<String, Float> docToken = docTokens.get(i);
            payloadBuilder.append(String.format("{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", index, startingId + i));
            payloadBuilder.append(System.lineSeparator());
            String strTokens = convertTokensToText(docToken);
            payloadBuilder.append(String.format("{ \"%s\": {%s}}", field, strTokens));
            payloadBuilder.append(System.lineSeparator());
        }
        bulkIngest(payloadBuilder.toString(), null);
    }

    private void ingestDocuments(String index, String field, List<Map<String, Float>> docTokens, List<String> docTexts, int startingId) {
        StringBuilder payloadBuilder = new StringBuilder();
        for (int i = 0; i < docTokens.size(); i++) {
            Map<String, Float> docToken = docTokens.get(i);
            payloadBuilder.append(String.format("{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", index, startingId + i));
            payloadBuilder.append(System.lineSeparator());
            String strTokens = convertTokensToText(docToken);
            payloadBuilder.append(String.format("{\"%s\": \"%s\", \"%s\": {%s}}", TEXT_FIELD, docTexts.get(i), field, strTokens));
            payloadBuilder.append(System.lineSeparator());
        }
        bulkIngest(payloadBuilder.toString(), null);
    }

    private Set<String> getDocIDs(Map<String, Object> searchResults) {
        Map<String, Object> hits1map = (Map<String, Object>) searchResults.get("hits");
        Set<String> actualIds = new HashSet<>();
        List<Object> hits1List = (List<Object>) hits1map.get("hits");
        for (Object hits1Object : hits1List) {
            Map<String, Object> mapObject = (Map<String, Object>) hits1Object;
            String id = mapObject.get("_id").toString();
            actualIds.add(id);
        }
        return actualIds;
    }

    private void waitForSegmentMerge(String index) throws InterruptedException {
        int maxRetry = 5;
        for (int i = 0; i < maxRetry; ++i) {
            if (1 == getSegmentCount(index)) {
                break;
            }
            Thread.sleep(1000);
        }
    }

    private int getSegmentCount(String index) {
        Request request = new Request("GET", "/_cat/segments/" + index);
        try {
            Response response = client().performRequest(request);
            String str = EntityUtils.toString(response.getEntity());
            String[] lines = str.split("\n");
            return lines.length;
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(String field, int cut, float hf, int k, Map<String, Float> query) {
        return getNeuralSparseQueryBuilder(field, cut, hf, k, query, null);
    }

    private NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(
        String field,
        int cut,
        float hf,
        int k,
        Map<String, Float> query,
        QueryBuilder filter
    ) {
        SparseAnnQueryBuilder annQueryBuilder = new SparseAnnQueryBuilder().queryCut(cut)
            .fieldName(field)
            .heapFactor(hf)
            .k(k)
            .queryTokens(query)
            .filter(filter);

        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder().sparseAnnQueryBuilder(annQueryBuilder)
            .fieldName(field)
            .queryTokensSupplier(() -> query);
        return neuralSparseQueryBuilder;
    }

    private void forceMerge(String indexName) throws IOException, ParseException {
        Request request = new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1");
        Response response = client().performRequest(request);
        String str = EntityUtils.toString(response.getEntity());
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
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

    private String makeDocument(Map<String, Float> tokens, String fieldName) {
        String tokenText = convertTokensToText(tokens);
        return String.format("{\n" + "\"passage_text\": \"apple tree\"," + "\"%s\": {%s}}", fieldName, tokenText);
    }

    private String convertTokensToText(Map<String, Float> tokens) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, Float> entry : tokens.entrySet()) {
            if (!first) {
                builder.append(",");
            }
            builder.append("\"");
            builder.append(entry.getKey());
            builder.append("\": ");
            builder.append(entry.getValue());
            first = false;
        }
        return builder.toString();
    }
}

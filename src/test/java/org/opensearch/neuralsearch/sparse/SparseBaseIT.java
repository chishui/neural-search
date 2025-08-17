/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldMapper;
import org.opensearch.neuralsearch.sparse.query.SparseAnnQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Base Integration tests for seismic feature
 */
public abstract class SparseBaseIT extends BaseNeuralSearchIT {

    protected static final String ALGO_NAME = SparseConstants.SEISMIC;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    protected void createSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold
    ) throws IOException {
        createSparseIndex(indexName, fieldName, nPostings, alpha, clusterRatio, approximateThreshold, 1, 0);
    }

    protected void createSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        int shards,
        int replicas
    ) throws IOException {
        Request request = configureSparseIndex(
            indexName,
            fieldName,
            nPostings,
            alpha,
            clusterRatio,
            approximateThreshold,
            shards,
            replicas
        );
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    private Request configureSparseIndex(
        String indexName,
        String fieldName,
        int nPostings,
        float alpha,
        float clusterRatio,
        int approximateThreshold,
        int shards,
        int replicas
    ) throws IOException {
        String indexSettings = prepareIndexSettings(shards, replicas);
        String indexMappings = prepareIndexMapping(nPostings, alpha, clusterRatio, approximateThreshold, fieldName);
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

    protected String convertTokensToText(Map<String, Float> tokens) {
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

    protected String prepareIndexSettings() throws IOException {
        return prepareIndexSettings(1, 0);
    }

    protected String prepareIndexSettings(int shards, int replicas) throws IOException {
        XContentBuilder settingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("index")
            .field("number_of_routing_shards", shards) // Shard routing setting
            .field("sparse", true)
            .field("number_of_shards", shards)
            .field("number_of_replicas", replicas)
            .endObject()
            .endObject();
        return settingBuilder.toString();
    }

    protected void forceMerge(String indexName) throws IOException, ParseException {
        Request request = new Request("POST", "/" + indexName + "/_forcemerge?max_num_segments=1");
        Response response = client().performRequest(request);
        String str = EntityUtils.toString(response.getEntity());
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    protected String prepareIndexMapping(int nPostings, float alpha, float clusterRatio, int approximateThreshold, String sparseFieldName)
        throws IOException {
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(sparseFieldName)
            .field("type", SparseTokensFieldMapper.CONTENT_TYPE)
            .startObject("method")
            .field("name", ALGO_NAME) // Integer: length of posting list
            .startObject("parameters")
            .field("n_postings", nPostings) // Integer: length of posting list
            .field("summary_prune_ratio", alpha) // Float
            .field("cluster_ratio", clusterRatio) // Float: cluster ratio
            .field("approximate_threshold", approximateThreshold)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        return mappingBuilder.toString();
    }

    protected void waitForSegmentMerge(String index) throws InterruptedException {
        waitForSegmentMerge(index, 1, 0);
    }

    protected void waitForSegmentMerge(String index, int shards, int replicas) throws InterruptedException {
        int maxRetry = 30;
        for (int i = 0; i < maxRetry; ++i) {
            if (shards * (1 + replicas) == getSegmentCount(index)) {
                return;
            }
            Thread.sleep(1000);
        }
    }

    protected int getSegmentCount(String index) {
        Request request = new Request("GET", "/_cat/segments/" + index);
        try {
            Response response = client().performRequest(request);
            String str = EntityUtils.toString(response.getEntity());
            String[] lines = str.split("\n");
            return lines.length;
        } catch (IOException | ParseException e) {
            return 0;
        }
    }

    protected int getNodeCount() {
        Request request = new Request("GET", "/_cat/nodes/");
        try {
            Response response = client().performRequest(request);
            String str = EntityUtils.toString(response.getEntity());
            String[] lines = str.split("\n");
            return lines.length;
        } catch (IOException | ParseException e) {
            return 0;
        }
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMerge(String index, String textField, String sparseField, List<Map<String, Float>> docTokens) {
        ingestDocumentsAndForceMerge(index, textField, sparseField, docTokens, null);
    }

    @SneakyThrows
    protected void ingestDocumentsAndForceMerge(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts
    ) {
        ingestDocuments(index, textField, sparseField, docTokens, docTexts, 1);

        forceMerge(index);
        // wait until force merge complete
        waitForSegmentMerge(index);
        assertEquals(1, getSegmentCount(index));
    }

    protected void ingestDocuments(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> text,
        int startingId
    ) {
        ingestDocuments(index, textField, sparseField, docTokens, text, startingId, null);
    }

    protected void ingestDocuments(
        String index,
        String textField,
        String sparseField,
        List<Map<String, Float>> docTokens,
        List<String> docTexts,
        int startingId,
        String routing
    ) {
        StringBuilder payloadBuilder = new StringBuilder();
        for (int i = 0; i < docTokens.size(); i++) {
            Map<String, Float> docToken = docTokens.get(i);
            payloadBuilder.append(
                String.format(Locale.ROOT, "{ \"index\": { \"_index\": \"%s\", \"_id\": \"%d\"} }", index, startingId + i)
            );
            payloadBuilder.append(System.lineSeparator());
            String strTokens = convertTokensToText(docToken);
            String text = CollectionUtils.isEmpty(docTexts) ? "text" : docTexts.get(i);
            payloadBuilder.append(String.format(Locale.ROOT, "{\"%s\": \"%s\", \"%s\": {%s}}", textField, text, sparseField, strTokens));
            payloadBuilder.append(System.lineSeparator());
        }
        bulkIngest(payloadBuilder.toString(), null, routing);
    }

    /**
     * Iterate from number 0 to 10000 and find num routing ids which can result in different shard id.
     *
     * @param num number of routing ids, should be <= shard number.
     * @return a list of routing ids
     */
    protected List<String> generateUniqueRoutingIds(int num) {
        List<String> routingIds = new ArrayList<>();
        Set<Integer> uniqueShardIds = new HashSet<>();
        for (int i = 0; i < 10000; ++i) {
            String candidate = String.valueOf(i);
            int hash = Murmur3HashFunction.hash(candidate);
            int shardId = Math.floorMod(hash, num);
            if (uniqueShardIds.contains(shardId)) {
                continue;
            }
            uniqueShardIds.add(shardId);
            routingIds.add(candidate);
            if (routingIds.size() == num) {
                break;
            }
        }
        return routingIds;
    }

    protected NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(String field, int cut, float hf, int k, Map<String, Float> query) {
        return getNeuralSparseQueryBuilder(field, cut, hf, k, query, null);
    }

    protected NeuralSparseQueryBuilder getNeuralSparseQueryBuilder(
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
}

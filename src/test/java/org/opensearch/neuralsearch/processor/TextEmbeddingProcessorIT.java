/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";
    private final String INGEST_DOC1 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc1.json").toURI()));
    private final String INGEST_DOC2 = Files.readString(Path.of(classLoader.getResource("processor/ingest_doc2.json").toURI()));
    private final String BULK_ITEM_TEMPLATE = Files.readString(
        Path.of(classLoader.getResource("processor/bulk_item_template.json").toURI())
    );

    public TextEmbeddingProcessorIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testTextEmbeddingProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
            createTextEmbeddingIndex();
            ingestDocument(INGEST_DOC1, null);
            assertEquals(1, getDocCount(INDEX_NAME));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessor_batch() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.TEXT_EMBEDDING);
            createTextEmbeddingIndex();
            ingestBatchDocumentWithBulk("batch_", 2, 2, Collections.EMPTY_SET, Collections.EMPTY_SET);
            assertEquals(2, getDocCount(INDEX_NAME));

            ingestDocument(String.format(LOCALE, INGEST_DOC1, "success"), "1");
            ingestDocument(String.format(LOCALE, INGEST_DOC2, "success"), "2");

            assertEquals(getDocById(INDEX_NAME, "1").get("_source"), getDocById(INDEX_NAME, "batch_1").get("_source"));
            assertEquals(getDocById(INDEX_NAME, "2").get("_source"), getDocById(INDEX_NAME, "batch_2").get("_source"));
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessor_withBatchSizeInProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
            Objects.requireNonNull(pipelineURLPath);
            String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
            createPipelineProcessor(requestBody, PIPELINE_NAME, modelId);
            createTextEmbeddingIndex();
            int docCount = 5;
            ingestBatchDocumentWithBulk("batch_", docCount, docCount, Collections.EMPTY_SET, Collections.EMPTY_SET);
            assertEquals(5, getDocCount(INDEX_NAME));

            for (int i = 0; i < docCount; ++i) {
                String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
                String payload = String.format(LOCALE, template, "success");
                ingestDocument(payload, String.valueOf(i + 1));
            }

            for (int i = 0; i < docCount; ++i) {
                assertEquals(
                    getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                    getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
                );

            }
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testTextEmbeddingProcessor_withFailureAndSkip() throws Exception {
        String modelId = null;
        try {
            modelId = uploadTextEmbeddingModel();
            loadModel(modelId);
            URL pipelineURLPath = classLoader.getResource("processor/PipelineConfigurationWithBatchSize.json");
            Objects.requireNonNull(pipelineURLPath);
            String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
            createPipelineProcessor(requestBody, PIPELINE_NAME, modelId);
            createTextEmbeddingIndex();
            int docCount = 5;
            ingestBatchDocumentWithBulk("batch_", docCount, docCount, Set.of(0), Set.of(1));
            assertEquals(3, getDocCount(INDEX_NAME));

            for (int i = 2; i < docCount; ++i) {
                String template = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
                String payload = String.format(LOCALE, template, "success");
                ingestDocument(payload, String.valueOf(i + 1));
            }

            for (int i = 2; i < docCount; ++i) {
                assertEquals(
                    getDocById(INDEX_NAME, String.valueOf(i + 1)).get("_source"),
                    getDocById(INDEX_NAME, "batch_" + (i + 1)).get("_source")
                );

            }
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndUploadModel(requestBody);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocument(String doc, String id) throws Exception {
        String endpoint;
        if (StringUtils.isEmpty(id)) {
            endpoint = INDEX_NAME + "/_doc?refresh";
        } else {
            endpoint = INDEX_NAME + "/_doc/" + id + "?refresh";
        }
        Response response = makeRequest(
            client(),
            "POST",
            endpoint,
            null,
            toHttpEntity(doc),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }

    private void ingestBatchDocumentWithBulk(String idPrefix, int docCount, int batchSize, Set<Integer> failedIds, Set<Integer> droppedIds)
        throws Exception {
        StringBuilder payloadBuilder = new StringBuilder();
        for (int i = 0; i < docCount; ++i) {
            String docTemplate = List.of(INGEST_DOC1, INGEST_DOC2).get(i % 2);
            if (failedIds.contains(i)) {
                docTemplate = String.format(LOCALE, docTemplate, "fail");
            } else if (droppedIds.contains(i)) {
                docTemplate = String.format(LOCALE, docTemplate, "drop");
            } else {
                docTemplate = String.format(LOCALE, docTemplate, "success");
            }
            String doc = docTemplate.replace("\n", "");
            final String id = idPrefix + (i + 1);
            String item = BULK_ITEM_TEMPLATE.replace("{{index}}", INDEX_NAME).replace("{{id}}", id).replace("{{doc}}", doc);
            payloadBuilder.append(item).append("\n");
        }
        final String payload = payloadBuilder.toString();
        Map<String, String> params = new HashMap<>();
        params.put("refresh", "true");
        params.put("batch_size", String.valueOf(batchSize));
        Response response = makeRequest(
            client(),
            "POST",
            "_bulk",
            params,
            toHttpEntity(payload),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        System.out.println(EntityUtils.toString(response.getEntity()));
        assertEquals(!failedIds.isEmpty(), map.get("errors"));
        assertEquals(docCount, ((List) map.get("items")).size());
    }
}

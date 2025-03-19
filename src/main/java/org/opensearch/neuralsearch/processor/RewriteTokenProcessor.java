/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.util.DocumentClusterManager;
import org.opensearch.neuralsearch.processor.util.DocumentClusterUtils;
import org.opensearch.neuralsearch.processor.util.JLTransformer;

import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor for rewriting token field to new token field
 */
public class RewriteTokenProcessor extends AbstractProcessor {
    public static final String TYPE = "rewrite_token";
    public static final String TOKEN_FIELD_KEY = "token_field";
    public static final String CLUSTER_ID = "cluster_id";
    public static final String SKETCH_TYPE = "sketch_type";
    private final String tokenField;

    protected RewriteTokenProcessor(String tag, String description, String tokenField) {
        super(tag, description);
        this.tokenField = tokenField;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Map<String, Float> tokens = ingestDocument.getFieldValue(tokenField, Map.class);
        String clusterId, sketchType = null;
        if (ingestDocument.hasField(SKETCH_TYPE)) {
            sketchType = ingestDocument.getFieldValue(SKETCH_TYPE, String.class);
        }
        if (ingestDocument.hasField(CLUSTER_ID)) {
            clusterId = ingestDocument.getFieldValue(CLUSTER_ID, String.class);
        } else {
            JLTransformer transformer = JLTransformer.getInstance();
            float[] sketch = DocumentClusterUtils.sparseToDense(tokens, 30109, transformer::convertSketchVector);
            int clusterIndex = DocumentClusterManager.getInstance().getTopCluster(sketch, sketchType);
            clusterId = DocumentClusterUtils.getClusterIdFromIndex(clusterIndex);
        }
        Map<String, Float> newTokens = tokens.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> DocumentClusterUtils.constructNewToken(e.getKey(), clusterId), Map.Entry::getValue));
        ingestDocument.setFieldValue(tokenField, newTokens);

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String tokenField = readStringProperty(TYPE, tag, config, TOKEN_FIELD_KEY);
            return new RewriteTokenProcessor(tag, description, tokenField);
        }
    }
}

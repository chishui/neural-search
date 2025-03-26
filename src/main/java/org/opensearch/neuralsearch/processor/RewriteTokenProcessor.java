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

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;
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
    private final String sketchType;

    protected RewriteTokenProcessor(String tag, String description, String tokenField, String sketchType) {
        super(tag, description);
        this.tokenField = tokenField;
        this.sketchType = sketchType;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Map<String, Float> tokens = ingestDocument.getFieldValue(tokenField, Map.class);
        Integer clusterId = null;
        if (!ingestDocument.hasField(CLUSTER_ID)) {
            float[] sketch = DocumentClusterUtils.sparseToDense(tokens, 30109, this.sketchType);
            clusterId = DocumentClusterManager.getInstance().getTopCluster(sketch, this.sketchType);
            if ("keyword".equals(DocumentClusterManager.getInstance().getClusterIdMethod())) {
                ingestDocument.setFieldValue(CLUSTER_ID, String.valueOf(clusterId));
            } else {
                ingestDocument.setFieldValue(CLUSTER_ID, clusterId);
            }
        }
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
            String sketchType = readOptionalStringProperty(TYPE, tag, config, SKETCH_TYPE);
            return new RewriteTokenProcessor(tag, description, tokenField, sketchType);
        }
    }
}

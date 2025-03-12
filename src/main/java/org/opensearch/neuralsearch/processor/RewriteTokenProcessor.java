/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.util.ClusterUtils;

import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;

/**
 * Processor for rewriting token field to new token field
 */
public class RewriteTokenProcessor extends AbstractProcessor {
    public static final String TYPE = "rewrite_token";
    public static final String TOKEN_FIELD_KEY = "token_field";
    public static final String CLUSTER_ID = "cluster_id";
    private String tokenField;

    protected RewriteTokenProcessor(String tag, String description, String tokenField) {
        super(tag, description);
        this.tokenField = tokenField;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        Map<String, Float> tokens = ingestDocument.getFieldValue(tokenField, Map.class);
        String clusterId = ingestDocument.getFieldValue(CLUSTER_ID, String.class);
        Map<String, Float> newTokens = tokens.entrySet()
            .stream()
            .collect(java.util.stream.Collectors.toMap(e -> ClusterUtils.constructNewToken(e.getKey(), clusterId), Map.Entry::getValue));
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

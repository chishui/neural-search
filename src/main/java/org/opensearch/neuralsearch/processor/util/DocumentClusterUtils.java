/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

import java.util.Map;
import java.util.Objects;

/**
 * Utility class for cluster related operations
 */
public class DocumentClusterUtils {

    public static String constructNewToken(String token, String clusterId) {
        return token + "_" + clusterId;
    }

    public static String getClusterIdFromIndex(int clusterIdx) {
        return String.valueOf(clusterIdx);
    }

    public static float[] sparseToDense(Map<String, Float> tokens, int denseDimension, String sketchType) {
        if (Objects.equals(sketchType, "Sinnamon")) {
            SinnamonTransformer sinnamonTransformer = SinnamonTransformer.getInstance();
            return sinnamonTransformer.convertSketchVector(tokens);
        }
        JLTransformer jlTransformer = JLTransformer.getInstance();
        float[] query = new float[denseDimension];
        for (Map.Entry<String, Float> entry : tokens.entrySet()) {
            double value = ((Number) entry.getValue()).doubleValue();
            query[Integer.parseInt(entry.getKey())] = (float) value;
        }
        return jlTransformer.convertSketchVector(query);
    }
}

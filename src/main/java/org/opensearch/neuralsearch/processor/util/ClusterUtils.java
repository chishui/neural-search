/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

/**
 * Utility class for cluster related operations
 */
public class ClusterUtils {
    public static String constructNewToken(String token, String clusterId) {
        return token + "_" + clusterId;
    }
}

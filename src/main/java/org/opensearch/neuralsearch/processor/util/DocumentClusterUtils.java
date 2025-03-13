/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.util;

public class DocumentClusterUtils {

    public static String constructNewToken(String token, String clusterId) {
        return token + "_" + clusterId;
    }
}

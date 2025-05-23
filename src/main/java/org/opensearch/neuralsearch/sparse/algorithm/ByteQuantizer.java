/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

public class ByteQuantizer {

    /**
     * Maps a positive float value to an unsigned integer within the range of the specified type.
     *
     * @param value The float value to map
     * @param maxValue The maximum float value to consider
     * @return The mapped unsigned integer value
     */
    public static byte mapPositiveFloatToByte(float value, float maxValue) {
        // Ensure the value is within the specified range
        value = Math.max(0.0f, Math.min(maxValue, value));

        // Scale the value to fit in the byte range (0-255)
        // Note: In Java, byte is signed (-128 to 127), so we'll use the half precision 0-127 range
        // by using unsigned operations
        float scaled = (value / maxValue) * 127.0f;

        // Round to nearest integer and cast to byte
        return (byte) Math.round(scaled);
    }
}

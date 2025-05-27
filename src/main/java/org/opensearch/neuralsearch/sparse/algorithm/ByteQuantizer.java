/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

public final class ByteQuantizer {

    private ByteQuantizer() {} // no instance of this utility class

    /**
     * Maps a positive float value to an unsigned integer within the range of the specified type.
     *
     * @param value The float value to map
     * @param maxValue The maximum float value to consider
     * @return The mapped unsigned integer value
     */
    public static byte quantizeFloatToByte(float value, float maxValue) {
        // Ensure the value is within the specified range
        value = Math.max(0.0f, Math.min(maxValue, value));

        // Scale the value to fit in the byte range (0-255)
        // Note: In Java, byte is signed (-128 to 127), but we'll use the full precision
        value = (value * 255.0f) / maxValue;

        // Round to nearest integer and cast to byte
        return (byte) Math.round(value);
    }
}

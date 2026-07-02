/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinaryVectorUtils {
    public static Map<Integer, Float> readToMap(BytesRef bytesRef) throws IOException {
        Map<Integer, Float> map = new HashMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int key = dis.readInt();
                float value = dis.readFloat();
                map.put(key, value);
            }
        }
        return map;
    }

    public static void readToList(BytesRef bytesRef, List<Integer> keys, List<Float> values) throws IOException {
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
            DataInputStream dis = new DataInputStream(bais)
        ) {
            while (bais.available() > 0) {
                int key = dis.readInt();
                float value = dis.readFloat();
                keys.add(key);
                values.add(value);
            }
        }
    }
}

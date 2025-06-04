/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sparse vector implementation, which is a list of (token, freq) pairs
 */
@EqualsAndHashCode
public class SparseVector implements Accountable {
    // tokens will be stored in order
    private short[] tokens;
    private float[] freqs;

    public SparseVector(BytesRef bytesRef) throws IOException {
        this(readToMap(bytesRef));
    }

    public int getSize() {
        return tokens == null ? 0 : tokens.length;
    }

    public short getDimension() {
        return tokens == null ? 0 : tokens[tokens.length - 1];
    }

    public SparseVector(Map<String, Float> pairs) {
        this(pairs.entrySet().stream().map(t -> new Item(convertStringToInteger(t.getKey()), t.getValue())).collect(Collectors.toList()));
    }

    private static Integer convertStringToInteger(String value) {
        return NumberUtils.createInteger(value);
    }

    public SparseVector(List<Item> items) {
        items.sort((o1, o2) -> o1.getToken() - o2.getToken());
        int size = items.size();
        this.tokens = new short[size];
        this.freqs = new float[size];
        for (int i = 0; i < size; ++i) {
            this.tokens[i] = (short) items.get(i).getToken();
            this.freqs[i] = items.get(i).getFreq();
        }
    }

    private static Map<String, Float> readToMap(BytesRef bytesRef) {
        Map<String, Float> map = new HashMap<>();
        try (
            ByteArrayInputStream bais = new ByteArrayInputStream(
                ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.length)
            );
            ObjectInputStream objectInputStream = new ObjectInputStream(bais)
        ) {
            while (bais.available() > 0) {
                String key = (String) objectInputStream.readObject();
                float value = objectInputStream.readFloat();
                map.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public float[] toDenseVector() {
        return toDenseVector(Short.MAX_VALUE);
    }

    public float[] toDenseVector(short dim) {
        int size = getSize();
        if (size == 0) {
            return new float[0];
        }

        int vectorDim;
        if (dim == Short.MAX_VALUE) {
            vectorDim = this.tokens[size - 1] + 1;
        } else {
            vectorDim = dim + 1;
        }

        float[] denseVector = new float[vectorDim];
        for (int i = 0; i < size; ++i) {
            if (this.tokens[i] < vectorDim) {
                denseVector[this.tokens[i]] = this.freqs[i];
            }
        }

        return denseVector;
    }

    public float dotProductAccelerated(final float[] denseVector) {
        int size = getSize();
        if (size == 0 || denseVector.length == 0) return 0;

        final int N_LANES = 4;
        float[] result = new float[N_LANES];

        final short[] tokens = this.tokens;
        final float[] freqs = this.freqs;

        final int limit = size - (size % N_LANES);
        int i = 0;

        for (; i < limit; i += N_LANES) {
            result[0] += freqs[i] * denseVector[tokens[i]];
            result[1] += freqs[i + 1] * denseVector[tokens[i + 1]];
            result[2] += freqs[i + 2] * denseVector[tokens[i + 2]];
            result[3] += freqs[i + 3] * denseVector[tokens[i + 3]];
        }

        for (; i < size; ++i) {
            result[0] += freqs[i] * denseVector[tokens[i]];
        }

        return result[0] + result[1] + result[2] + result[3];
    }

    public float dotProduct(final float[] denseVector) {
        float score = 0.0f;
        int size = getSize();
        // Early exit for empty vectors
        if (size == 0 || denseVector.length == 0) return 0;

        // Loop unrolling for better performance
        final int unrollFactor = 4;
        final int limit = size - (size % unrollFactor);

        // Main loop with unrolling
        int i = 0;
        for (; i < limit; i += unrollFactor) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += this.freqs[i] * denseVector[this.tokens[i]];

            if (this.tokens[i + 1] >= denseVector.length) {
                ++i;
                break;
            }
            score += this.freqs[i + 1] * denseVector[this.tokens[i + 1]];

            if (this.tokens[i + 2] >= denseVector.length) {
                i += 2;
                break;
            }
            score += this.freqs[i + 2] * denseVector[this.tokens[i + 2]];

            if (this.tokens[i + 3] >= denseVector.length) {
                i += 3;
                break;
            }
            score += this.freqs[i + 3] * denseVector[this.tokens[i + 3]];
        }

        // Handle remaining elements
        for (; i < size; ++i) {
            if (this.tokens[i] >= denseVector.length) {
                break;
            }
            score += this.freqs[i] * denseVector[this.tokens[i]];
        }

        return score;
    }

    public IteratorWrapper<Item> iterator() {
        return new IteratorWrapper<>(new Iterator<>() {
            private int size = getSize();
            private int current = -1;

            @Override
            public boolean hasNext() {
                return current + 1 < size;
            }

            @Override
            public Item next() {
                if (!hasNext()) {
                    return null;
                }
                ++current;
                return new Item(tokens[current], freqs[current]);
            }
        });
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOfInstance(SparseVector.class) + RamUsageEstimator.sizeOf(tokens) + RamUsageEstimator.sizeOf(
            freqs
        );
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static class Item {
        int token;
        float freq;

        static Item of(int token, float freq) {
            return new Item(token, freq);
        }
    }
}

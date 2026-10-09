/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.apache.lucene.util.BitUtil;

/**
 * Little-endian scalar accessors over a byte array holding a slice of the nsparse file.
 *
 * <p>A block, and a cluster summary's values, are read as one contiguous byte array and then walked
 * in place -- the same thing the C++ reader does by casting into its mapping. These are the
 * casts. Going through {@link BitUtil}'s {@code VarHandle}s rather than hand-shifting keeps them a
 * single machine load, and Lucene's own readers use exactly these.
 */
final class NsparseCodes {

    private NsparseCodes() {}

    /** The {@code uint32_t} at {@code offset}, as an int; a count above 2^31 is not a count. */
    static int intAt(byte[] bytes, int offset) {
        return (int) BitUtil.VH_LE_INT.get(bytes, offset);
    }

    /** The {@code uint16_t} at {@code offset}, widened so a term id is never sign-extended. */
    static int unsignedShortAt(byte[] bytes, int offset) {
        return (short) BitUtil.VH_LE_SHORT.get(bytes, offset) & 0xFFFF;
    }

    /** The {@code float} at {@code offset}. */
    static float floatAt(byte[] bytes, int offset) {
        return (float) BitUtil.VH_LE_FLOAT.get(bytes, offset);
    }

    /**
     * One stored code, widened to float. The stride selects the interpretation the way the C++
     * distance kernels dispatch on it: 1 and 2 bytes are unsigned integer codes, 4 is a float.
     */
    static float codeAt(byte[] bytes, int offset, int elementSize) {
        if (elementSize == 1) {
            return bytes[offset] & 0xFF;
        }
        if (elementSize == 2) {
            return unsignedShortAt(bytes, offset);
        }
        return floatAt(bytes, offset);
    }
}

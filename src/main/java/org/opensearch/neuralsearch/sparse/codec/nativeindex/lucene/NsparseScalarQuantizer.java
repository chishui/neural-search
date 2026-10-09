/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import java.io.IOException;
import java.util.Locale;

/**
 * Min-max scalar quantizer, a transcription of nsparse's {@code ScalarQuantizer}.
 *
 * <p>It has to round exactly as the C++ one does, because the codes it produces for a query are
 * compared against codes the C++ writer stored. {@code std::lround} and {@link Math#round(float)}
 * agree over the clamped range {@code [0, max]}, both rounding a tie away from zero, so a query
 * weight lands on the same code either way.
 */
final class NsparseScalarQuantizer {

    private static final float MAX_8BIT = 255.0f;
    private static final float MAX_16BIT = 65535.0f;

    private final int quantizerType;
    private final float vmin;
    private final float vmax;

    /**
     * @param quantizerType {@link NsparseFormat#QUANTIZER_8BIT} or {@link NsparseFormat#QUANTIZER_16BIT}
     * @param vmin          low end of the quantization range
     * @param vmax          high end of the quantization range
     * @throws IOException if the file declares a type or a range no writer could have produced.
     *                     {@code bytes_per_value()} treats anything but 8-bit as 16-bit, so an
     *                     undefined type would otherwise silently pick a stride; and a non-finite or
     *                     inverted range makes every code and every decoded score NaN while the
     *                     ranking still looks plausible.
     */
    NsparseScalarQuantizer(int quantizerType, float vmin, float vmax) throws IOException {
        if (quantizerType != NsparseFormat.QUANTIZER_8BIT && quantizerType != NsparseFormat.QUANTIZER_16BIT) {
            throw new IOException("Native sparse index declares an unknown quantizer type " + quantizerType);
        }
        if (Float.isFinite(vmin) == false || Float.isFinite(vmax) == false || vmax <= vmin) {
            throw new IOException(
                String.format(Locale.ROOT, "Native sparse index declares an unusable quantization range [%f, %f]", vmin, vmax)
            );
        }
        this.quantizerType = quantizerType;
        this.vmin = vmin;
        this.vmax = vmax;
    }

    /** Bytes each stored code occupies; the stride of every values array in the file. */
    int bytesPerValue() {
        return quantizerType == NsparseFormat.QUANTIZER_8BIT ? 1 : 2;
    }

    int quantizerType() {
        return quantizerType;
    }

    /**
     * A quantizer of this index's type over a different range, which is what a query is encoded
     * with: the type is always the index's, since the codes are compared against stored ones, but
     * the range may be the search ceiling rather than the ingest one.
     */
    NsparseScalarQuantizer withRange(float queryVmin, float queryVmax) throws IOException {
        return new NsparseScalarQuantizer(quantizerType, queryVmin, queryVmax);
    }

    /** Encodes {@code values} into codes, widened to int so an 8-bit code is never sign-extended. */
    int[] encode(float[] values) {
        float max = quantizerType == NsparseFormat.QUANTIZER_8BIT ? MAX_8BIT : MAX_16BIT;
        float scale = max / (vmax - vmin);
        int[] codes = new int[values.length];
        for (int i = 0; i < values.length; ++i) {
            float scaled = (values[i] - vmin) * scale;
            codes[i] = Math.round(Math.max(0.0f, Math.min(max, scaled)));
        }
        return codes;
    }

    /**
     * Scales an integer dot product of stored codes against query codes back to the float scale,
     * the way {@code decode_dot_product} does.
     *
     * @param quantizedScore the raw integer dot product
     * @param querySq        the quantizer the query was encoded with
     */
    float decodeDotProduct(float quantizedScore, NsparseScalarQuantizer querySq) {
        float max = quantizerType == NsparseFormat.QUANTIZER_8BIT ? MAX_8BIT : MAX_16BIT;
        float scale = ((vmax - vmin) * (querySq.vmax - querySq.vmin)) / (max * max);
        return quantizedScore * scale;
    }
}

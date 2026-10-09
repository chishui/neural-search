/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class NsparseScalarQuantizerTests extends OpenSearchTestCase {

    public void testEightBitCodesSpanTheRange() throws IOException {
        NsparseScalarQuantizer quantizer = new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 0.0f, 4.0f);

        assertEquals(1, quantizer.bytesPerValue());
        assertArrayEquals(new int[] { 0, 64, 128, 255 }, quantizer.encode(new float[] { 0.0f, 1.0f, 2.0f, 4.0f }));
    }

    /** Values outside the range clamp rather than wrap, or a heavy weight would encode as a light one. */
    public void testCodesClampToTheRange() throws IOException {
        NsparseScalarQuantizer quantizer = new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 0.0f, 2.0f);

        assertArrayEquals(new int[] { 0, 255 }, quantizer.encode(new float[] { -5.0f, 100.0f }));
    }

    public void testSixteenBitCodesUseTheWiderRange() throws IOException {
        NsparseScalarQuantizer quantizer = new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_16BIT, 0.0f, 1.0f);

        assertEquals(2, quantizer.bytesPerValue());
        assertArrayEquals(new int[] { 0, 32768, 65535 }, quantizer.encode(new float[] { 0.0f, 0.5f, 1.0f }));
    }

    /**
     * A dot product of codes decodes by the product of the two ranges over the code range squared, so a
     * query quantized at a wider ceiling than the documents still lands on the float scale.
     */
    public void testDecodesADotProductBackToTheFloatScale() throws IOException {
        NsparseScalarQuantizer ingest = new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 0.0f, 3.0f);
        NsparseScalarQuantizer search = ingest.withRange(0.0f, 16.0f);

        // one component: a doc weight of 3.0 encodes to 255, a query weight of 16.0 to 255
        float decoded = ingest.decodeDotProduct(255.0f * 255.0f, search);

        assertEquals(3.0f * 16.0f, decoded, 1e-3f);
    }

    public void testWithRangeKeepsTheIndexTypeAndOnlyChangesTheRange() throws IOException {
        NsparseScalarQuantizer sixteenBit = new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_16BIT, 0.0f, 3.0f);

        NsparseScalarQuantizer query = sixteenBit.withRange(0.0f, 16.0f);

        assertEquals(sixteenBit.quantizerType(), query.quantizerType());
        assertEquals(2, query.bytesPerValue());
    }

    /**
     * A type the file could declare but no writer produces has to be rejected: bytes_per_value treats
     * anything but 8-bit as 16-bit, so it would otherwise silently pick a stride.
     */
    public void testRejectsAnUnknownQuantizerType() {
        expectThrows(IOException.class, () -> new NsparseScalarQuantizer(7, 0.0f, 1.0f));
    }

    /** A NaN or inverted range makes every code and every score NaN while the ranking looks plausible. */
    public void testRejectsAnUnusableRange() {
        expectThrows(IOException.class, () -> new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 0.0f, Float.NaN));
        expectThrows(IOException.class, () -> new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 1.0f, 1.0f));
        expectThrows(IOException.class, () -> new NsparseScalarQuantizer(NsparseFormat.QUANTIZER_8BIT, 0.0f, Float.POSITIVE_INFINITY));
    }
}

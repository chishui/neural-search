/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.opensearch.test.OpenSearchTestCase;

public class NsparseFormatTests extends OpenSearchTestCase {

    /** A fourcc is four printable characters on the wire, whatever its numeric value reads as. */
    public void testFourccReadsAsItsCharacters() {
        assertEquals("DSSQ", NsparseFormat.fourccToString(NsparseFormat.FOURCC_DSSQ));
        assertEquals("IDMP", NsparseFormat.fourccToString(NsparseFormat.FOURCC_IDMP));
    }

    public void testPaddingReachesTheNextBoundary() {
        assertEquals(0, NsparseFormat.paddingFor(0, 8));
        assertEquals(7, NsparseFormat.paddingFor(1, 8));
        assertEquals(0, NsparseFormat.paddingFor(16, 8));
        assertEquals(1, NsparseFormat.paddingFor(3, 2));
        assertEquals("an alignment of one never pads", 0, NsparseFormat.paddingFor(3, 1));
    }

    /** The element width can come from the file, so a zero has to throw rather than divide by zero. */
    public void testRejectsAZeroAlignment() {
        expectThrows(IllegalArgumentException.class, () -> NsparseFormat.paddingFor(3, 0));
    }

    public void testAlignUpIsIdempotentOnABoundary() {
        assertEquals(24, NsparseFormat.alignUp(24, 8));
        assertEquals(4096, NsparseFormat.alignUp(25, 4096));
    }

    public void testPowerOfTwo() {
        assertTrue(NsparseFormat.isPowerOfTwo(4096));
        assertTrue(NsparseFormat.isPowerOfTwo(8));
        assertFalse(NsparseFormat.isPowerOfTwo(0));
        assertFalse(NsparseFormat.isPowerOfTwo(12));
        assertFalse(NsparseFormat.isPowerOfTwo(-8));
    }

    /**
     * The block layout has to be the same function the C++ writer used, since a block is validated by
     * comparing its computed end against the length the directory recorded.
     */
    public void testBlockOffsetsMatchTheWrittenLayout() {
        // 2 docs, 5 nonzeros, 1-byte values: [u32 n_docs][u32 doc_id x2][u32 off x3][u16 comps x5][vals x5]
        long[] offsets = NsparseFormat.blockOffsets(2, 5, 1);

        assertEquals(4, offsets[NsparseFormat.OFFSET_DOC_IDS]);
        assertEquals(12, offsets[NsparseFormat.OFFSET_OFF]);
        assertEquals(24, offsets[NsparseFormat.OFFSET_COMPS]);
        assertEquals("1-byte values need no padding after comps", 34, offsets[NsparseFormat.OFFSET_VALS]);
        assertEquals(39, offsets[NsparseFormat.OFFSET_END]);
    }

    /** Values are read as element-wide words, so they start on that boundary, not on the comps' end. */
    public void testValuesArePaddedToTheirElementWidth() {
        // 1 doc, 3 nonzeros, 2-byte values: comps start at 16 and end at 22, already 2-aligned
        assertEquals(22, NsparseFormat.blockOffsets(1, 3, 2)[NsparseFormat.OFFSET_VALS]);
        // 2 docs, 3 nonzeros, 4-byte values: comps end at 24 + 6 = 30, padded up to 32
        assertEquals(32, NsparseFormat.blockOffsets(2, 3, 4)[NsparseFormat.OFFSET_VALS]);
    }

    public void testAnEmptyBlockIsJustItsPrefixAndOneOffset() {
        long[] offsets = NsparseFormat.blockOffsets(0, 0, 1);

        assertEquals(NsparseFormat.INLINE_BLOCK_PREFIX_BYTES + Integer.BYTES, offsets[NsparseFormat.OFFSET_END]);
    }
}

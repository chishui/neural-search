/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import java.util.Locale;

/**
 * On-disk constants of the nsparse index file, as the C++ writer lays them out.
 *
 * <p>The engine file is written straight into a Lucene {@link org.apache.lucene.store.IndexOutput}
 * by nsparse (see {@link org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper}), so the payload
 * starts at offset 0 and only a Lucene footer follows it. Everything is host byte order, which is
 * little-endian on every platform the plugin ships for -- the same order Lucene's
 * {@link org.apache.lucene.store.DataInput} reads in, so no swapping is needed.
 *
 * <p>The layout is rebuildable, not compatible across versions: nsparse offers no cross-version
 * guarantee and the file name carries {@link org.opensearch.neuralsearch.sparse.algorithm.SparseEngine#version()},
 * so a mismatch is caught by the name rather than tolerated here. These constants mirror
 * {@code nsparse/io/io.h}, {@code nsparse/inline_forward_index.h} and
 * {@code nsparse/disk_seismic_index_base.h}; any change there must be mirrored here and the engine
 * version bumped.
 */
final class NsparseFormat {

    private NsparseFormat() {}

    /** {@code IndexHeader}: u32 fourcc, u32 payload version, i32 dimension, written unpadded. */
    static final int INDEX_HEADER_BYTES = 12;

    /** The {@code IDMapIndex} wrapper the plugin always builds ({@code idmap: true}). */
    static final int FOURCC_IDMP = fourcc("IDMP");
    /** {@code DiskSeismicScalarQuantizedIndex}, what a {@code per_block} field builds. */
    static final int FOURCC_DSSQ = fourcc("DSSQ");

    /** Highest {@code IDMapIndex} payload version this reader lays out. */
    static final int IDMP_MAX_VERSION = 1;
    /** Highest {@code DiskSeismicScalarQuantizedIndex} payload version this reader lays out. */
    static final int DSSQ_MAX_VERSION = 1;

    /** {@code detail::kMinBlockAlign}: the boundary inline-forward blocks and its section start on. */
    static final long MIN_BLOCK_ALIGN = 8;

    /** {@code InlineForwardIndexHeader}: u32 element_size, 4 bytes of struct padding, u64 n_blocks, u64 page_size. */
    static final int INLINE_HEADER_BYTES = 24;
    /** {@code InlineForwardIndexTrailer}: u64 dir_offset, u64 n_entries, u64 n_lists. */
    static final int INLINE_TRAILER_BYTES = 24;
    /** {@code InlineDirEntry}: u32 pl, u32 block, u64 byte_off, u64 len, u32 n_docs, u32 reserved. */
    static final int INLINE_DIR_ENTRY_BYTES = 32;
    /** A block's leading {@code [u32 n_docs]}, before its {@code doc_id[]} array. */
    static final int INLINE_BLOCK_PREFIX_BYTES = 4;
    /** Width of a component id on the wire ({@code term_t} is {@code uint16_t}). */
    static final int INLINE_COMP_BYTES = 2;

    /** {@code detail::DocLocator}: u32 posting_list, u32 block, u32 slot. */
    static final int DOC_LOCATOR_BYTES = 12;
    /** {@code DocLocator::kRemainder}: the doc is in no block, and lives in the remainder store. */
    static final long DOC_LOCATOR_REMAINDER = 0xFFFF_FFFFL;

    /** {@code QuantizerType::QT_8bit}, one byte per stored code. */
    static final int QUANTIZER_8BIT = 0;
    /** {@code QuantizerType::QT_16bit}, two bytes per stored code. */
    static final int QUANTIZER_16BIT = 1;

    /**
     * The four characters of an index id as the u32 nsparse writes, so a constant here reads the
     * same way it does in the file.
     */
    private static int fourcc(String id) {
        assert id.length() == 4 : id;
        return (id.charAt(0) & 0xFF) | (id.charAt(1) & 0xFF) << 8 | (id.charAt(2) & 0xFF) << 16 | (id.charAt(3) & 0xFF) << 24;
    }

    /** An id as it reads in the file, for an error message; its numeric value names nothing. */
    static String fourccToString(int id) {
        char[] chars = new char[4];
        for (int i = 0; i < 4; ++i) {
            chars[i] = (char) ((id >>> (8 * i)) & 0xFF);
        }
        return new String(chars);
    }

    /** Bytes to skip at {@code pos} to reach an {@code alignment} boundary ({@code io_align::padding_for}). */
    static long paddingFor(long pos, long alignment) {
        if (alignment <= 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "native sparse index declares a zero element width at offset %d", pos)
            );
        }
        return (alignment - pos % alignment) % alignment;
    }

    /** {@code detail::inline_align_up}. */
    static long alignUp(long offset, long alignment) {
        return offset + paddingFor(offset, alignment);
    }

    /** Whether {@code value} is a power of two, which every alignment read out of the file must be. */
    static boolean isPowerOfTwo(long value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    /**
     * Byte offsets, relative to a block's start, of each of its structure-of-arrays sub-arrays
     * ({@code detail::inline_block_offsets}). {@code end} is the block's payload length, which the
     * directory entry must agree with.
     *
     * @param nDocs       documents in the block
     * @param totalNnz    {@code off[n_docs]}, the block's nonzero count
     * @param elementSize stored value width in bytes
     */
    static long[] blockOffsets(long nDocs, long totalNnz, long elementSize) {
        long docIds = INLINE_BLOCK_PREFIX_BYTES;
        long off = docIds + nDocs * Integer.BYTES;
        long comps = off + (nDocs + 1) * Integer.BYTES;
        long vals = alignUp(comps + totalNnz * INLINE_COMP_BYTES, elementSize);
        long end = vals + totalNnz * elementSize;
        return new long[] { docIds, off, comps, vals, end };
    }

    /** Index into the array {@link #blockOffsets} returns. */
    static final int OFFSET_DOC_IDS = 0;
    /** Index into the array {@link #blockOffsets} returns. */
    static final int OFFSET_OFF = 1;
    /** Index into the array {@link #blockOffsets} returns. */
    static final int OFFSET_COMPS = 2;
    /** Index into the array {@link #blockOffsets} returns. */
    static final int OFFSET_VALS = 3;
    /** Index into the array {@link #blockOffsets} returns. */
    static final int OFFSET_END = 4;
}

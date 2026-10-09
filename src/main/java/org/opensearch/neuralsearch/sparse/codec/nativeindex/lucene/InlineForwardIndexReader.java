/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

import java.io.IOException;
import java.util.Locale;

/**
 * The inline (block-contiguous) forward index, read through an {@link IndexInput}.
 *
 * <p>Each document's vector is copied into every block it belongs to, so a block is one contiguous
 * read of everything needed to score its documents -- which is the whole point of the {@code
 * per_block} layout, and the reason this reader can serve a query with one read per selected block
 * rather than one per document.
 *
 * <p>Only the directory is held in RAM, as the C++ reader also does: the {@code (pl, block)} to byte
 * range table, which is 20 bytes per block against a corpus-sized payload. Block payloads stay on
 * disk and are read on demand by {@link #readBlock}, so residency is the directory's page cache
 * behaviour, not the plugin's heap.
 *
 * <p>Fail-closed: every directory entry is validated at load and every block's interior against its
 * entry when read, so a lookup can return an absent view but never an out-of-bounds one.
 */
final class InlineForwardIndexReader {

    /** A block is read whole into a byte array, so its length has to fit one. */
    private static final long MAX_BLOCK_BYTES = ArrayUtil.MAX_ARRAY_LENGTH;

    /** Absolute file offset the section starts at; a block's {@code byte_off} is relative to it. */
    private final long sectionBase;
    private final int elementSize;
    private final int numLists;
    /** Where each list's entries start in the parallel arrays below; {@code numLists + 1} long. */
    private final int[] listOffset;
    private final long[] blockOffset;
    private final int[] blockLength;
    private final int[] blockNumDocs;

    private InlineForwardIndexReader(
        long sectionBase,
        int elementSize,
        int numLists,
        int[] listOffset,
        long[] blockOffset,
        int[] blockLength,
        int[] blockNumDocs
    ) {
        this.sectionBase = sectionBase;
        this.elementSize = elementSize;
        this.numLists = numLists;
        this.listOffset = listOffset;
        this.blockOffset = blockOffset;
        this.blockLength = blockLength;
        this.blockNumDocs = blockNumDocs;
    }

    /**
     * Reads the section the cursor is positioned at and leaves the cursor just past it, mirroring
     * {@code InlineForwardIndex::mmap_deserialize}: the section is self-delimiting, so it can be
     * composed at any stream offset.
     */
    static InlineForwardIndexReader read(NsparseCursor cursor) throws IOException {
        cursor.skipPadding(NsparseFormat.MIN_BLOCK_ALIGN);
        long sectionLength = cursor.readUnsignedLong("inline forward section length");
        long base = cursor.position();
        if (base + sectionLength > cursor.payloadEnd()) {
            throw corrupt("section length " + sectionLength + " overruns the payload");
        }
        if (sectionLength < NsparseFormat.INLINE_HEADER_BYTES + NsparseFormat.INLINE_TRAILER_BYTES) {
            throw corrupt("section of " + sectionLength + " bytes is too small to hold a header and a trailer");
        }

        // Header: u32 element_size, four bytes of struct padding, u64 n_blocks, u64 page_size.
        cursor.seek(base);
        int elementSize = cursor.readInt();
        if (elementSize != 1 && elementSize != 2 && elementSize != 4) {
            throw corrupt("bad element size " + elementSize);
        }
        cursor.skip(Integer.BYTES);
        long declaredBlocks = cursor.readUnsignedLong("block");
        long pageSize = cursor.readUnsignedLong("page size");
        if (pageSize < NsparseFormat.MIN_BLOCK_ALIGN || NsparseFormat.isPowerOfTwo(pageSize) == false) {
            throw corrupt("bad page size " + pageSize);
        }

        long directoryEnd = sectionLength - NsparseFormat.INLINE_TRAILER_BYTES;
        cursor.seek(base + directoryEnd);
        long directoryOffset = cursor.readUnsignedLong("directory offset");
        long numEntries = cursor.readUnsignedLong("directory entry");
        long numLists = cursor.readUnsignedLong("posting list");

        if (numEntries != declaredBlocks
            || directoryOffset < NsparseFormat.INLINE_HEADER_BYTES
            || directoryOffset > directoryEnd
            || numEntries > MAX_BLOCK_BYTES) {
            throw corrupt("corrupt directory");
        }
        // Compared by division so an oversized entry count cannot overflow a multiply.
        long directoryBytes = directoryEnd - directoryOffset;
        if (directoryBytes % NsparseFormat.INLINE_DIR_ENTRY_BYTES != 0
            || directoryBytes / NsparseFormat.INLINE_DIR_ENTRY_BYTES != numEntries) {
            throw corrupt("corrupt directory");
        }
        // A posting list id derives from a term id, so it lives in the term space; bounding it there
        // keeps the per-list table below a bounded allocation.
        if (numLists > 1 << 16) {
            throw corrupt("implausible posting list count " + numLists);
        }

        int entryCount = (int) numEntries;
        int listCount = (int) numLists;
        long[] blockOffset = new long[entryCount];
        int[] blockLength = new int[entryCount];
        int[] blockNumDocs = new int[entryCount];
        int[] listOffset = new int[listCount + 1];

        cursor.seek(base + directoryOffset);
        boolean started = false;
        int previousList = 0;
        int previousBlock = 0;
        for (int i = 0; i < entryCount; ++i) {
            int listId = cursor.readInt();
            int blockId = cursor.readInt();
            long byteOffset = cursor.readUnsignedLong("block offset");
            long length = cursor.readUnsignedLong("block length");
            int numDocs = cursor.readInt();
            cursor.skip(Integer.BYTES); // reserved
            if (listId < 0
                || listId >= listCount
                || numDocs < 0
                || length < NsparseFormat.INLINE_BLOCK_PREFIX_BYTES
                || length > MAX_BLOCK_BYTES
                || byteOffset % NsparseFormat.MIN_BLOCK_ALIGN != 0
                || byteOffset < NsparseFormat.INLINE_HEADER_BYTES
                || byteOffset > directoryOffset
                || directoryOffset - byteOffset < length) {
                throw corrupt("corrupt directory entry " + i);
            }
            // Grouped by (list ascending, block ascending, gap-free), which is what makes
            // (pl, block) -> entry the O(1) prefix-sum lookup below. A new list restarts block
            // numbering at 0.
            int expectedBlock = (started == false || listId != previousList) ? 0 : previousBlock + 1;
            if ((started && listId < previousList) || blockId != expectedBlock) {
                throw corrupt("directory is not ordered by (posting list, block) at entry " + i);
            }
            started = true;
            previousList = listId;
            previousBlock = blockId;
            blockOffset[i] = base + byteOffset;
            blockLength[i] = (int) length;
            blockNumDocs[i] = numDocs;
            ++listOffset[listId + 1];
        }
        for (int i = 0; i < listCount; ++i) {
            listOffset[i + 1] += listOffset[i];
        }

        cursor.seek(base + sectionLength);
        return new InlineForwardIndexReader(base, elementSize, listCount, listOffset, blockOffset, blockLength, blockNumDocs);
    }

    int elementSize() {
        return elementSize;
    }

    int numBlocks() {
        return blockOffset.length;
    }

    int numLists() {
        return numLists;
    }

    /** How many blocks list {@code pl} has, or 0 if it has none or does not exist. */
    int numBlocksInList(int pl) {
        if (pl < 0 || pl >= numLists) {
            return 0;
        }
        return listOffset[pl + 1] - listOffset[pl];
    }

    /** The directory entry for {@code (pl, block)}, or -1 when there is none. */
    int entryOf(int pl, int block) {
        if (block < 0 || block >= numBlocksInList(pl)) {
            return -1;
        }
        return listOffset[pl] + block;
    }

    /**
     * Asks the directory to start fetching a block's bytes without waiting for them, so the reads of
     * a query's selected blocks can overlap. What that costs and buys is the directory's business: a
     * memory-mapped one issues {@code madvise(WILL_NEED)}, one over a block cache can start an
     * asynchronous fill, and one that cannot do either ignores it.
     */
    void prefetchBlock(IndexInput input, int entry) throws IOException {
        input.prefetch(blockOffset[entry], blockLength[entry]);
    }

    /**
     * Reads block {@code entry} whole and returns a validated view over it.
     *
     * @param input   an {@link IndexInput} over the engine file, positioned anywhere
     * @param entry   a directory entry index from {@link #entryOf}
     * @param scratch a buffer to read into, reused across blocks and grown as needed
     */
    InlineBlockView readBlock(IndexInput input, int entry, BlockScratch scratch) throws IOException {
        int length = blockLength[entry];
        byte[] bytes = scratch.grow(length);
        input.seek(blockOffset[entry]);
        input.readBytes(bytes, 0, length);
        return view(bytes, length, blockNumDocs[entry], entry);
    }

    /**
     * Validates a block's interior against its directory entry and builds a view over it.
     *
     * <p>One length equality does most of the work: the sub-array offsets are a pure function of
     * {@code n_docs} and {@code off[n_docs]}, so a block whose computed end matches the length the
     * directory recorded cannot have an array reaching outside it.
     */
    private InlineBlockView view(byte[] bytes, int length, int expectedNumDocs, int entry) throws IOException {
        int numDocs = NsparseCodes.intAt(bytes, 0);
        if (numDocs != expectedNumDocs) {
            throw corrupt("block " + entry + " declares " + numDocs + " docs, its directory entry " + expectedNumDocs);
        }
        // doc_id[] and off[] have to fit before the offsets can be read; where comps starts does not
        // depend on the nonzero count, so it is computable first.
        long[] header = NsparseFormat.blockOffsets(numDocs, 0, elementSize);
        if (header[NsparseFormat.OFFSET_COMPS] > length) {
            throw corrupt("block " + entry + " header overruns its " + length + " bytes");
        }
        int offsetsBase = (int) header[NsparseFormat.OFFSET_OFF];
        if (NsparseCodes.intAt(bytes, offsetsBase) != 0) {
            throw corrupt("block " + entry + " offsets do not start at 0");
        }
        for (int i = 0; i < numDocs; ++i) {
            if (Integer.compareUnsigned(
                NsparseCodes.intAt(bytes, offsetsBase + (i + 1) * Integer.BYTES),
                NsparseCodes.intAt(bytes, offsetsBase + i * Integer.BYTES)
            ) < 0) {
                throw corrupt("block " + entry + " offsets are not monotonic");
            }
        }
        long totalNnz = Integer.toUnsignedLong(NsparseCodes.intAt(bytes, offsetsBase + numDocs * Integer.BYTES));
        long[] layout = NsparseFormat.blockOffsets(numDocs, totalNnz, elementSize);
        if (layout[NsparseFormat.OFFSET_END] != length) {
            throw corrupt("block " + entry + " lays out " + layout[NsparseFormat.OFFSET_END] + " bytes, its entry records " + length);
        }
        return new InlineBlockView(
            bytes,
            numDocs,
            (int) layout[NsparseFormat.OFFSET_DOC_IDS],
            offsetsBase,
            (int) layout[NsparseFormat.OFFSET_COMPS],
            (int) layout[NsparseFormat.OFFSET_VALS],
            elementSize
        );
    }

    /** Bytes of heap the directory occupies, for the cache accounting that owns this reader. */
    long ramBytesUsed() {
        return RamUsageEstimator.sizeOf(blockOffset) + RamUsageEstimator.sizeOf(blockLength) + RamUsageEstimator.sizeOf(blockNumDocs)
            + RamUsageEstimator.sizeOf(listOffset);
    }

    private static IOException corrupt(String message) {
        return new IOException("Corrupt native sparse inline forward index: " + message);
    }

    /** A grow-on-demand read buffer, held by a searcher and reused across the blocks of a query. */
    static final class BlockScratch {
        private byte[] bytes = new byte[0];

        byte[] grow(int length) {
            if (bytes.length < length) {
                bytes = new byte[ArrayUtil.oversize(length, Byte.BYTES)];
            }
            return bytes;
        }
    }

    /**
     * A read-only view of one block's structure-of-arrays (a within-block CSR) over the buffer it was
     * read into. Valid until the next {@link #readBlock} on the same scratch buffer.
     */
    static final class InlineBlockView {
        private final byte[] bytes;
        private final int numDocs;
        private final int docIdsBase;
        private final int offsetsBase;
        private final int compsBase;
        private final int valsBase;
        private final int elementSize;

        private InlineBlockView(byte[] bytes, int numDocs, int docIdsBase, int offsetsBase, int compsBase, int valsBase, int elementSize) {
            this.bytes = bytes;
            this.numDocs = numDocs;
            this.docIdsBase = docIdsBase;
            this.offsetsBase = offsetsBase;
            this.compsBase = compsBase;
            this.valsBase = valsBase;
            this.elementSize = elementSize;
        }

        int numDocs() {
            return numDocs;
        }

        /** The global (index-internal) doc id in slot {@code slot}. */
        int docId(int slot) {
            return NsparseCodes.intAt(bytes, docIdsBase + slot * Integer.BYTES);
        }

        /** Where slot {@code slot}'s components start within the block's {@code comps}/{@code vals}. */
        private int offset(int slot) {
            return NsparseCodes.intAt(bytes, offsetsBase + slot * Integer.BYTES);
        }

        /** How many components slot {@code slot} has. */
        int nnz(int slot) {
            return offset(slot + 1) - offset(slot);
        }

        /**
         * Dots slot {@code slot}'s vector against a dense query lookup table, giving the raw
         * (still quantized) score. Codes on both sides are compared at the width the file stores
         * them in, so the table holds {@code elementSize} bytes per component and must cover the
         * whole {@code uint16_t} component space -- see {@link DiskSeismicSqSearcher}, which sizes
         * it. That is what makes this loop bounds-check-free against a component id the file
         * supplies.
         */
        float dotProduct(int slot, byte[] dense) {
            int start = offset(slot);
            int end = offset(slot + 1);
            float score = 0.0f;
            for (int i = start; i < end; ++i) {
                int component = NsparseCodes.unsignedShortAt(bytes, compsBase + i * NsparseFormat.INLINE_COMP_BYTES);
                score += NsparseCodes.codeAt(bytes, valsBase + i * elementSize, elementSize) * NsparseCodes.codeAt(
                    dense,
                    component * elementSize,
                    elementSize
                );
            }
            return score;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "InlineBlockView(numDocs=%d, nnz=%d)", numDocs, numDocs == 0 ? 0 : offset(numDocs));
        }
    }
}

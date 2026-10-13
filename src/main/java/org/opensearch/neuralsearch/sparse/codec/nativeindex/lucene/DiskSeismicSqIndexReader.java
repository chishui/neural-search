/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.util.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Reads the {@code .nsparse} engine file of a {@code per_block} sparse field through a Lucene
 * {@link IndexInput}, rather than by handing nsparse a filesystem path to {@code mmap}.
 *
 * <p>The file holds an nsparse {@code IDMapIndex} wrapping a {@code DiskSeismicScalarQuantizedIndex}
 * ({@code disk_seismic_sq}), followed by a Lucene footer. This class parses that layout: the id map
 * and the cluster summaries into RAM, the inline forward index's directory into RAM, and the block
 * payloads left on disk for {@link DiskSeismicSqSearcher} to read as a query selects them.
 *
 * <p>Why it exists: the native engine loads an index by path, so it needs an
 * {@link org.apache.lucene.store.FSDirectory} whose bytes are the plaintext index. A directory that
 * decrypts or fetches in {@code openInput} -- there is no path whose contents are the index -- has no
 * way to serve that, and {@code disk_seismic_sq} has no copying reader in nsparse either
 * ("DiskSeismic index is mmap-only"). Parsing the format here is the same answer k-NN reached for
 * faiss: an {@code IndexInput}-driven reader, with any raw-pointer fast path an accelerator rather
 * than a requirement.
 *
 * <p>Not a replacement for the native path in general: only {@code disk_seismic_sq} is understood
 * here, so a segment below the field's {@code approximate_threshold} -- which the writer builds as an
 * nsparse {@code inverted} index instead -- is not readable through this class. The caller checks
 * that before choosing it.
 *
 * <p>One instance is shared by every query on a segment core and is thread-safe: all mutable
 * per-query state lives in the searcher, which clones {@link #cloneInput()} for its own reads.
 */
@Log4j2
public final class DiskSeismicSqIndexReader implements Closeable {

    /**
     * Opening walks every posting list's summary header front to back, one page per list; without
     * readahead that is one storage round trip per page, which on network storage made opening a
     * segment take minutes.
     */
    private static final IOContext OPEN_CONTEXT = IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.SEQUENTIAL);
    /** A block payload is random-access, so the directory should not read ahead of it. */
    private static final IOContext CONTEXT = IOContext.DEFAULT.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM);

    private final IndexInput input;
    /** The nsparse payload ends where the Lucene footer starts. */
    private final long payloadEnd;

    /** Highest component id plus one, over the documents of this segment. */
    @Getter
    private final int dimension;
    /** Documents the index holds; also the length of the id map. */
    @Getter
    private final int numVectors;
    /** Internal (dense, 0-based) id to Lucene doc id, ascending. */
    private final int[] internalToExternal;
    private final NsparseScalarQuantizer quantizer;
    /**
     * Absolute file offset of each posting list's summary record, indexed by component id. The records
     * themselves stay on disk: a query reads the {@code cut} it needs (see {@link ClusterSummaries}).
     */
    private final long[] summaryOffset;
    /** Clusters per posting list, so a searcher can size its accumulator without reading the record. */
    private final int[] summaryClusters;
    /** End of the summary section, which bounds the last list's record. */
    private final long summaryEnd;
    private final InlineForwardIndexReader forwardIndex;
    /** Where the per-document locator table starts, or -1 when the index is empty. */
    private final long docLocatorsOffset;
    private final RemainderVectors remainder;

    private DiskSeismicSqIndexReader(
        IndexInput input,
        long payloadEnd,
        int dimension,
        int numVectors,
        int[] internalToExternal,
        NsparseScalarQuantizer quantizer,
        long[] summaryOffset,
        int[] summaryClusters,
        long summaryEnd,
        InlineForwardIndexReader forwardIndex,
        long docLocatorsOffset,
        RemainderVectors remainder
    ) {
        this.input = input;
        this.payloadEnd = payloadEnd;
        this.dimension = dimension;
        this.numVectors = numVectors;
        this.internalToExternal = internalToExternal;
        this.quantizer = quantizer;
        this.summaryOffset = summaryOffset;
        this.summaryClusters = summaryClusters;
        this.summaryEnd = summaryEnd;
        this.forwardIndex = forwardIndex;
        this.docLocatorsOffset = docLocatorsOffset;
        this.remainder = remainder;
    }

    /**
     * Opens and parses the engine file.
     *
     * <p>A segment in which no document has the field gets a footer-only file (see
     * {@code DefaultNativeIndexWriter}), which parses to an empty index rather than an error.
     *
     * @param directory the directory holding the file; for a compound segment this is the real
     *                  directory, since the engine file is kept out of the {@code .cfs}
     * @param fileName  the engine file's name in that directory
     */
    public static DiskSeismicSqIndexReader open(Directory directory, String fileName) throws IOException {
        IndexInput input = directory.openInput(fileName, OPEN_CONTEXT);
        boolean success = false;
        try {
            DiskSeismicSqIndexReader reader = parse(input);
            // Queries read scattered blocks; readahead there would evict the cache they rely on.
            input.updateIOContext(CONTEXT);
            success = true;
            return reader;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(input);
            }
        }
    }

    private static DiskSeismicSqIndexReader parse(IndexInput input) throws IOException {
        long payloadEnd = input.length() - CodecUtil.footerLength();
        if (payloadEnd <= 0) {
            // Footer only: the field is mapped but no document in this segment has it.
            return new DiskSeismicSqIndexReader(
                input,
                Math.max(payloadEnd, 0),
                0,
                0,
                new int[0],
                null,
                new long[0],
                new int[0],
                0,
                null,
                -1,
                RemainderVectors.EMPTY
            );
        }
        NsparseCursor cursor = new NsparseCursor(input, payloadEnd);

        // The plugin always builds an IDMapIndex (idmap: true), whose payload is the internal-to-doc-id
        // map followed by the delegate index with its own header.
        readIndexHeader(cursor, NsparseFormat.FOURCC_IDMP, NsparseFormat.IDMP_MAX_VERSION);
        int mapSize = cursor.readCount("id map");
        int[] internalToExternal = new int[mapSize];
        for (int i = 0; i < mapSize; ++i) {
            internalToExternal[i] = cursor.readInt();
            // Ascending by construction -- the writer streams doc values in doc order -- and relied on
            // by the reverse lookup a filtered query needs, so it is checked rather than assumed.
            if (i > 0 && internalToExternal[i] <= internalToExternal[i - 1]) {
                throw new IOException("Native sparse index id map is not ascending at entry " + i);
            }
        }

        int dimension = readIndexHeader(cursor, NsparseFormat.FOURCC_DSSQ, NsparseFormat.DSSQ_MAX_VERSION);
        // The quantization header opens the DSSQ payload, ahead of the layout the two disk-resident
        // indexes share.
        NsparseScalarQuantizer quantizer = new NsparseScalarQuantizer(cursor.readByte(), cursor.readFloat(), cursor.readFloat());
        int numVectors = cursor.readCount("vector");
        if (numVectors != mapSize) {
            throw new IOException("Native sparse index holds " + numVectors + " vectors but an id map of " + mapSize);
        }

        // One pass over the summary section reading only its scalars: enough to record where each
        // posting list's record starts and to reject a stored width the quantizer disagrees with, without
        // putting any of the section on the heap.
        int numLists = cursor.readCount("posting list");
        long[] summaryOffset = new long[numLists];
        int[] summaryClusters = new int[numLists];
        for (int i = 0; i < numLists; ++i) {
            summaryOffset[i] = cursor.position();
            summaryClusters[i] = ClusterSummaries.skip(cursor, quantizer.bytesPerValue());
        }
        long summaryEnd = cursor.position();
        InlineForwardIndexReader forwardIndex = InlineForwardIndexReader.read(cursor);
        validateForwardWidth(quantizer, forwardIndex);

        // Doc-locator directory, then the vectors of the documents that are in no block, in the order
        // write_doc_directory wrote them.
        cursor.skipPadding(NsparseFormat.MIN_BLOCK_ALIGN);
        int numLocators = cursor.readCount("doc locator");
        if (numLocators != numVectors) {
            throw new IOException(
                "Native sparse index doc-locator count " + numLocators + " disagrees with its vector count " + numVectors
            );
        }
        cursor.skipPadding(Integer.BYTES);
        long docLocatorsOffset = cursor.position();
        cursor.skip((long) numLocators * NsparseFormat.DOC_LOCATOR_BYTES);
        RemainderVectors remainder = RemainderVectors.read(cursor);

        return new DiskSeismicSqIndexReader(
            input,
            payloadEnd,
            dimension,
            numVectors,
            internalToExternal,
            quantizer,
            summaryOffset,
            summaryClusters,
            summaryEnd,
            forwardIndex,
            docLocatorsOffset,
            remainder
        );
    }

    /** Reads an {@code IndexHeader}, checks the id and version, and returns the dimension. */
    private static int readIndexHeader(NsparseCursor cursor, int expectedId, int maxVersion) throws IOException {
        int id = cursor.readInt();
        int version = cursor.readInt();
        int dimension = cursor.readInt();
        if (id != expectedId) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Native sparse index declares type [%s] where [%s] was expected",
                    NsparseFormat.fourccToString(id),
                    NsparseFormat.fourccToString(expectedId)
                )
            );
        }
        // A version outside 1..maxVersion is one this reader cannot lay out; parsing on would consume
        // whatever the fields happen to align with.
        if (version < 1 || version > maxVersion) {
            throw new IOException(
                String.format(
                    Locale.ROOT,
                    "Native sparse index declares [%s] format version %d; this reader reads 1 through %d",
                    NsparseFormat.fourccToString(id),
                    version,
                    maxVersion
                )
            );
        }
        if (dimension < 0) {
            throw new IOException("Native sparse index declares a negative dimension " + dimension);
        }
        return dimension;
    }

    /**
     * Rejects a file whose stored forward codes are not the width its quantizer implies; they are
     * strided at that width on the read path, so a disagreement would be read at the wrong stride and
     * score plausible nonsense. The summaries are checked the same way during the offset walk.
     */
    private static void validateForwardWidth(NsparseScalarQuantizer quantizer, InlineForwardIndexReader forwardIndex) throws IOException {
        if (forwardIndex.numBlocks() > 0 && forwardIndex.elementSize() != quantizer.bytesPerValue()) {
            throw new IOException(
                "Native sparse index forward element size " + forwardIndex.elementSize() + " disagrees with its quantizer type"
            );
        }
    }

    /**
     * Whether this index can return a hit at all. An empty segment cannot, and neither can one whose
     * inline forward index has no blocks: a mapped disk-resident index has no in-RAM vectors to fall
     * back to, so with no blocks there is nothing to score -- the same early return the C++ search
     * takes, ahead of even its exact-match path.
     */
    public boolean isEmpty() {
        return numVectors == 0 || forwardIndex == null || forwardIndex.numBlocks() == 0;
    }

    NsparseScalarQuantizer quantizer() {
        return quantizer;
    }

    /** Where the summary section ends, i.e. where the last list's record ends. */
    long summaryEnd() {
        return summaryEnd;
    }

    /** Where component {@code component}'s summary record starts, or -1 if it has no posting list. */
    long summaryOffset(int component) {
        return component >= 0 && component < summaryOffset.length ? summaryOffset[component] : -1;
    }

    /** Clusters in component {@code component}'s posting list, 0 if it has none. */
    int summaryClusters(int component) {
        return component >= 0 && component < summaryClusters.length ? summaryClusters[component] : 0;
    }

    int numPostingLists() {
        return summaryOffset.length;
    }

    InlineForwardIndexReader forwardIndex() {
        return forwardIndex;
    }

    long docLocatorsOffset() {
        return docLocatorsOffset;
    }

    RemainderVectors remainder() {
        return remainder;
    }

    /** The Lucene doc id of an internal id, or -1 when the id is not one this index holds. */
    int toDocId(int internalId) {
        return internalId >= 0 && internalId < internalToExternal.length ? internalToExternal[internalId] : -1;
    }

    /**
     * The internal id of a Lucene doc id, or -1 when the document is not in this index. A binary
     * search, which the ascending id map makes possible -- the C++ index keeps a hash map for the same
     * lookup, which at corpus scale is heap this reader would rather not hold.
     */
    int toInternalId(int docId) {
        int position = Arrays.binarySearch(internalToExternal, docId);
        return position >= 0 ? position : -1;
    }

    /**
     * An independent {@link IndexInput} over the engine file, for one searcher's reads. Cloning is
     * what makes concurrent searches on a shared reader safe: they must not share a file pointer.
     */
    IndexInput cloneInput() {
        return input.clone();
    }

    long payloadEnd() {
        return payloadEnd;
    }

    /**
     * Bytes of heap this reader holds: the id map, the per-list summary offsets, and the block
     * directory. Not the summaries or the block payloads, which stay on disk.
     */
    public long ramBytesUsed() {
        long bytes = RamUsageEstimator.sizeOf(internalToExternal) + RamUsageEstimator.sizeOf(summaryOffset) + RamUsageEstimator.sizeOf(
            summaryClusters
        );
        return bytes + (forwardIndex == null ? 0 : forwardIndex.ramBytesUsed());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(input);
    }

    /**
     * The full vectors of the documents that were pruned from every block, kept on disk and read one
     * row at a time: only a filtered query small enough to run an exact match ever needs them.
     *
     * @param count        rows the store holds
     * @param elementSize  stored value width
     * @param indptrOffset absolute file offset of the {@code offset_t[count + 1]} row boundaries
     * @param indicesOffset absolute file offset of the {@code uint16_t[]} component ids
     * @param valuesOffset absolute file offset of the values
     */
    record RemainderVectors(int count, int elementSize, long indptrOffset, long indicesOffset, long valuesOffset) {

        static final RemainderVectors EMPTY = new RemainderVectors(0, 1, -1, -1, -1);

        /**
         * Walks the {@code SparseVectors} section the cursor is positioned at without reading its
         * arrays, recording where each one starts. The row boundaries are the only array whose length
         * the header does not give, so one value of it is read to place the two that follow.
         */
        static RemainderVectors read(NsparseCursor cursor) throws IOException {
            int count = cursor.readCount("remainder vector");
            if (count == 0) {
                return EMPTY;
            }
            cursor.readCount("remainder dimension");
            int elementSize = cursor.readCount("remainder element size");
            if (elementSize != 1 && elementSize != 2 && elementSize != 4) {
                throw new IOException("Native sparse index declares a remainder element size of " + elementSize);
            }
            cursor.skipPadding(Long.BYTES);
            long indptrOffset = cursor.position();
            cursor.skip((long) count * Long.BYTES);
            long totalNnz = cursor.readUnsignedLong("remainder nonzero");
            cursor.skipPadding(Short.BYTES);
            long indicesOffset = cursor.position();
            cursor.skip(totalNnz * Short.BYTES);
            cursor.skipPadding(elementSize);
            long valuesOffset = cursor.position();
            cursor.skip(totalNnz * elementSize);
            return new RemainderVectors(count, elementSize, indptrOffset, indicesOffset, valuesOffset);
        }
    }
}

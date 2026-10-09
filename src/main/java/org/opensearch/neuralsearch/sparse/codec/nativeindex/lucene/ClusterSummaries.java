/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * One posting list's cluster summaries, read from disk on demand into reusable buffers.
 *
 * <p>A query dots itself against the summaries of the {@code cut} posting lists its heaviest tokens
 * name, to decide which blocks are worth reading. That is {@code cut} lists per query -- three by
 * default -- out of one list per term in the vocabulary. So the summaries are read per query rather
 * than held: on MS MARCO at {@code lambda=6000, beta=400} the whole summary section is 4.3 GiB across
 * 30,109 lists, which no plugin should put on the JVM heap, while the three lists a query needs are
 * about 160 KiB each.
 *
 * <p>That is the one place the C++ reader's residency argument does not carry over. It borrows the
 * summaries from its mapping, so holding all of them costs it nothing; copying them onto the heap
 * instead is what an earlier version of this class did, and it OOM-killed a node with a 2 GiB heap on
 * the first query. {@link DiskSeismicSqIndexReader} therefore keeps only each list's byte offset --
 * 30,109 longs -- and one of these is reused across the lists of a query.
 *
 * <p>Stored term-major (CSC), as {@code InvertedListClusters} writes it: for the i-th distinct summary
 * term {@code termIds[i]}, entries {@code [termPtr[i], termPtr[i + 1])} of the CSC cluster/value arrays
 * hold that term's (cluster id, summary value) pairs. Locating a term by binary search rather than by a
 * dimension-sized table is what keeps a list's footprint proportional to its nonzeros.
 */
final class ClusterSummaries {

    private int numClusters;
    private int elementSize;
    private int numTerms;
    private int nnz;
    /** Distinct summary terms, ascending; only {@code [0, numTerms)} is live. */
    private int[] termIds = new int[0];
    /** CSC offsets; only {@code [0, numTerms]} is live. */
    private int[] termPtr = new int[0];
    /** Cluster id per entry, as raw little-endian {@code uint16} pairs. */
    private byte[] cscCluster = new byte[0];
    /** Summary value per entry, {@code elementSize} bytes each. */
    private byte[] cscValue = new byte[0];

    int numClusters() {
        return numClusters;
    }

    int elementSize() {
        return elementSize;
    }

    /**
     * Walks one list's record without keeping its arrays, leaving the cursor just past it. Used at open
     * to find where each list starts and to check its stored width.
     *
     * @param cursor              positioned at the start of a list's record
     * @param expectedElementSize the width the index's quantizer implies, or 0 to accept any
     * @return how many clusters the list has
     */
    static int skip(NsparseCursor cursor, int expectedElementSize) throws IOException {
        Header header = readHeader(cursor, expectedElementSize);
        if (header.numTerms > 0) {
            cursor.skipPaddedArray(header.numTerms, Short.BYTES);
            cursor.skipPaddedArray(header.numTerms + 1, Integer.BYTES);
        }
        int nnz = cursor.readCount("summary entry");
        cursor.skipPaddedArray(nnz, Short.BYTES);
        cursor.skipPaddedArray(Math.multiplyExact(nnz, header.elementSize), header.elementSize);
        return header.numClusters;
    }

    /**
     * Reads the list whose record starts at the cursor's position into this object's buffers,
     * replacing whatever it held.
     */
    void read(NsparseCursor cursor, int expectedElementSize) throws IOException {
        Header header = readHeader(cursor, expectedElementSize);
        numClusters = header.numClusters;
        elementSize = header.elementSize;
        numTerms = header.numTerms;
        if (numTerms > 0) {
            termIds = grow(termIds, numTerms);
            cursor.skipPadding(Short.BYTES);
            for (int i = 0; i < numTerms; ++i) {
                termIds[i] = cursor.readShort() & 0xFFFF;
            }
            termPtr = grow(termPtr, numTerms + 1);
            cursor.skipPadding(Integer.BYTES);
            for (int i = 0; i <= numTerms; ++i) {
                termPtr[i] = cursor.readInt();
            }
        }
        nnz = cursor.readCount("summary entry");
        cscCluster = grow(cscCluster, Math.multiplyExact(nnz, Short.BYTES));
        cursor.skipPadding(Short.BYTES);
        cursor.readBytes(cscCluster, 0, nnz * Short.BYTES);
        int valueBytes = Math.multiplyExact(nnz, elementSize);
        cscValue = grow(cscValue, valueBytes);
        cursor.skipPadding(elementSize);
        cursor.readBytes(cscValue, 0, valueBytes);
        validate();
    }

    /** The scalars every list's record opens with, shared by {@link #skip} and {@link #read}. */
    private record Header(int numClusters, int elementSize, int numTerms) {
    }

    private static Header readHeader(NsparseCursor cursor, int expectedElementSize) throws IOException {
        // The doc-id membership that opens the record is skipped: a per_block index writes it as two
        // count-0 arrays (serialize_summaries_only) because the inline forward index already carries it,
        // and the search path never needs it. Reading the counts and skipping whatever they declare
        // keeps this correct for a list that does carry membership.
        int numDocs = cursor.readCount("summary doc");
        cursor.skipPaddedArray(numDocs, Integer.BYTES);
        int numOffsets = cursor.readCount("summary offset");
        cursor.skipPaddedArray(numOffsets, Integer.BYTES);

        int numClusters = cursor.readCount("cluster");
        int elementSize = cursor.readCount("summary element size");
        if (elementSize != 1 && elementSize != 2 && elementSize != 4) {
            throw new IOException("Native sparse index declares a summary element size of " + elementSize);
        }
        // The stored codes are strided at the width the quantizer reports, so a list that disagrees
        // would be read at the wrong stride and score plausible nonsense.
        if (expectedElementSize > 0 && numClusters > 0 && elementSize != expectedElementSize) {
            throw new IOException("Native sparse index summary element size " + elementSize + " disagrees with its quantizer type");
        }
        // A cluster id is uint16 on the wire, so a list cannot hold more than that many clusters.
        if (numClusters > 1 << 16) {
            throw new IOException("Native sparse index declares " + numClusters + " clusters in one posting list");
        }
        int numTerms = cursor.readCount("summary term");
        return new Header(numClusters, elementSize, numTerms);
    }

    /**
     * Rejects a record whose offsets do not describe its entries. {@link #scoreSummaries} indexes the
     * CSC arrays by a file-supplied offset and {@code out} by a file-supplied cluster id, so both have
     * to be in range before any of it runs, not while it does.
     */
    private void validate() throws IOException {
        if (numTerms == 0) {
            return;
        }
        if (termPtr[0] != 0 || termPtr[numTerms] != nnz) {
            throw new IOException("Native sparse index has cluster summary offsets that do not span its entries");
        }
        for (int i = 0; i < numTerms; ++i) {
            if (termPtr[i + 1] < termPtr[i]) {
                throw new IOException("Native sparse index has non-monotonic cluster summary offsets");
            }
            if (i > 0 && termIds[i] <= termIds[i - 1]) {
                throw new IOException("Native sparse index has unordered cluster summary terms");
            }
        }
    }

    /**
     * Accumulates this list's per-cluster summary scores for the query's (term, code) pairs into
     * {@code out}, zeroing it over {@code [0, numClusters())} first. Terms absent from the summaries
     * contribute nothing.
     *
     * @param queryTerms token ids of the query, in any order
     * @param queryCodes the query's quantized weights, aligned with {@code queryTerms}
     * @param out        per-cluster accumulator, at least {@link #numClusters()} long
     */
    void scoreSummaries(int[] queryTerms, int[] queryCodes, float[] out) throws IOException {
        Arrays.fill(out, 0, numClusters, 0.0f);
        if (numClusters == 0 || numTerms == 0) {
            return;
        }
        for (int i = 0; i < queryTerms.length; ++i) {
            int column = Arrays.binarySearch(termIds, 0, numTerms, queryTerms[i]);
            if (column < 0) {
                continue;
            }
            float queryValue = queryCodes[i];
            int end = termPtr[column + 1];
            for (int j = termPtr[column]; j < end; ++j) {
                int cluster = NsparseCodes.unsignedShortAt(cscCluster, j * Short.BYTES);
                if (cluster >= numClusters) {
                    throw new IOException("Native sparse index has a cluster summary entry for cluster " + cluster);
                }
                out[cluster] += queryValue * NsparseCodes.codeAt(cscValue, j * elementSize, elementSize);
            }
        }
    }

    private static int[] grow(int[] array, int length) {
        return array.length >= length ? array : new int[ArrayUtil.oversize(length, Integer.BYTES)];
    }

    private static byte[] grow(byte[] array, int length) {
        return array.length >= length ? array : new byte[ArrayUtil.oversize(length, Byte.BYTES)];
    }
}

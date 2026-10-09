/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.io.IOException;
import java.util.Arrays;

/**
 * Runs a SEISMIC block-budget search over a {@link DiskSeismicSqIndexReader}, in Java.
 *
 * <p>The algorithm is nsparse's {@code DiskSeismicIndexBase::search}: quantize the query at the
 * search range, take its {@code cut} heaviest tokens, dot the query against the cluster summaries of
 * those tokens' posting lists, read the globally best {@code k'} blocks, and score every document in
 * them exactly. A fixed block budget rather than a relative threshold is what keeps the number of
 * reads per query flat, which is the property a disk-resident index is built around.
 *
 * <p>Two things differ from the C++ search, both deliberate:
 * <ul>
 *   <li><b>Reads are prefetched, then consumed.</b> The selected blocks' byte ranges are handed to
 *       the directory before any of them is read, so a directory that can overlap I/O -- an
 *       asynchronous block cache, {@code madvise} on a mapping -- has the chance to. The C++ path
 *       faults them in one at a time through the mapping.
 *   <li><b>Query tokens outside the component space are dropped rather than scattered.</b> The JNI
 *       wrapper narrows tokens to {@code uint16_t} but deliberately does not bound them by the index
 *       dimension, which leaves a scatter past the end of its dense buffer; the table here spans the
 *       whole {@code uint16_t} range, so there is nothing to bound and nothing to overrun.
 * </ul>
 *
 * <p>Not thread-safe: one searcher per query. It holds a cloned {@link IndexInput} and the scratch
 * buffers a query reuses across its blocks, which is what keeps the hot path allocation-free.
 */
public final class DiskSeismicSqSearcher {

    private static final SparseQueryResult[] NO_HITS = new SparseQueryResult[0];

    /** Component ids are {@code uint16_t}, so a table over the whole space needs no bounds check. */
    private static final int COMPONENT_SPACE = 1 << 16;

    /** How many documents a query is guessed to visit, for the visited set's initial table. */
    private static final int VISITED_SIZE_HINT = 4096;

    private final DiskSeismicSqIndexReader reader;
    private final IndexInput input;
    /** Over the same cloned input, for the scalar-and-padding walk a summary record needs. */
    private final NsparseCursor cursor;
    /** Reused across the cut lists of a query, and across queries. */
    private final ClusterSummaries summaries = new ClusterSummaries();
    private final int elementSize;

    /** Dense query lookup, {@link #elementSize} bytes per component; sparse in use, cleared per query. */
    private final byte[] dense;
    private final VisitedDocSet visited = new VisitedDocSet(VISITED_SIZE_HINT);
    private final InlineForwardIndexReader.BlockScratch blockScratch = new InlineForwardIndexReader.BlockScratch();
    private final InlineForwardIndexReader.BlockScratch componentScratch = new InlineForwardIndexReader.BlockScratch();
    private final InlineForwardIndexReader.BlockScratch valueScratch = new InlineForwardIndexReader.BlockScratch();

    private float[] summaryScores = new float[0];
    private float[] candidateScores = new float[0];
    private int[] candidateBlocks = new int[0];

    public DiskSeismicSqSearcher(DiskSeismicSqIndexReader reader) throws IOException {
        this.reader = reader;
        this.input = reader.cloneInput();
        this.cursor = new NsparseCursor(input, reader.payloadEnd());
        this.elementSize = reader.isEmpty() ? 1 : reader.forwardIndex().elementSize();
        if (elementSize != 1 && elementSize != 2) {
            // A disk_seismic_sq index stores 8- or 16-bit codes. Four bytes would mean float-valued
            // forward vectors, which only the unquantized DiskSeismicIndex writes and which the query
            // encoder here has no counterpart for.
            throw new IOException("Native sparse index stores " + elementSize + "-byte forward values, which this reader cannot score");
        }
        this.dense = new byte[COMPONENT_SPACE * elementSize];
    }

    /**
     * Returns the query's hits, highest score first, with Lucene doc ids -- the same shape and order
     * the native path returns, padding entries excluded.
     */
    public SparseQueryResult[] search(DiskSeismicSqQuery query) throws IOException {
        if (reader.isEmpty() || query.tokens().length == 0 || query.k() <= 0) {
            return NO_HITS;
        }
        if (query.kPrime() <= 0) {
            throw new IllegalArgumentException("k_prime (block budget) must be positive, got " + query.kPrime());
        }

        // A token a component id cannot hold matches nothing, so dropping it changes no score --
        // whereas narrowing it would alias two distinct tokens onto one component and score them
        // together.
        int length = 0;
        int[] terms = new int[query.tokens().length];
        float[] weights = new float[query.tokens().length];
        for (int i = 0; i < query.tokens().length; ++i) {
            int token = query.tokens()[i];
            if (token >= 0 && token < COMPONENT_SPACE) {
                terms[length] = token;
                weights[length] = query.weights()[i];
                ++length;
            }
        }
        if (length == 0) {
            return NO_HITS;
        }
        terms = ArrayUtil.copyOfSubArray(terms, 0, length);
        weights = ArrayUtil.copyOfSubArray(weights, 0, length);

        NsparseScalarQuantizer querySq = reader.quantizer().withRange(query.vmin(), query.vmax());
        int[] codes = querySq.encode(weights);

        int[] internalFilter = query.filterDocIds() == null ? null : toInternalIds(query.filterDocIds());
        // An enumerable filter of at most k members must return every one of them, so those documents
        // are scored directly rather than only where the top blocks happen to cover them. The decision
        // uses the filter's own size, as the native side does, not how many of its ids this segment
        // holds.
        if (internalFilter != null && query.filterDocIds().length <= query.k()) {
            return exactMatch(terms, codes, internalFilter, query.k(), querySq);
        }
        return blockBudgetSearch(terms, codes, internalFilter, query, querySq);
    }

    private SparseQueryResult[] blockBudgetSearch(
        int[] terms,
        int[] codes,
        int[] internalFilter,
        DiskSeismicSqQuery query,
        NsparseScalarQuantizer querySq
    ) throws IOException {
        int[] cutTerms = cutTerms(terms, codes, query.cut());
        int candidateCount = collectCandidates(cutTerms, terms, codes);
        if (candidateCount == 0) {
            return NO_HITS;
        }
        int[] selected = selectBudget(candidateCount, query.kPrime());

        InlineForwardIndexReader forwardIndex = reader.forwardIndex();
        // Two passes: hand every block's range to the directory, then read them. A directory that can
        // start these fetches concurrently does; one that cannot has ignored the hint by now.
        int[] entries = new int[selected.length];
        int entryCount = 0;
        for (int block : selected) {
            int entry = forwardIndex.entryOf(block >>> 16, block & 0xFFFF);
            if (entry >= 0) {
                entries[entryCount++] = entry;
                forwardIndex.prefetchBlock(input, entry);
            }
        }

        scatterQuery(terms, codes);
        visited.clear();
        TopScoreHeap hits = new TopScoreHeap(query.k());
        try {
            for (int i = 0; i < entryCount; ++i) {
                InlineForwardIndexReader.InlineBlockView block = forwardIndex.readBlock(input, entries[i], blockScratch);
                for (int slot = 0; slot < block.numDocs(); ++slot) {
                    int internalId = block.docId(slot);
                    if (visited.add(internalId) == false) {
                        continue;
                    }
                    if (internalFilter != null && Arrays.binarySearch(internalFilter, internalId) < 0) {
                        continue;
                    }
                    hits.add(block.dotProduct(slot, dense), internalId);
                }
            }
        } finally {
            clearQuery(terms);
        }
        return toResults(hits, querySq);
    }

    /**
     * Scores exactly the filtered documents, resolving each through the per-document locator table:
     * a slot of one of the blocks it was copied into, or a row of the remainder store when it was
     * pruned from every block.
     */
    private SparseQueryResult[] exactMatch(int[] terms, int[] codes, int[] internalIds, int k, NsparseScalarQuantizer querySq)
        throws IOException {
        if (internalIds.length == 0) {
            return NO_HITS;
        }
        long locatorsOffset = reader.docLocatorsOffset();
        for (int internalId : internalIds) {
            input.prefetch(locatorsOffset + (long) internalId * NsparseFormat.DOC_LOCATOR_BYTES, NsparseFormat.DOC_LOCATOR_BYTES);
        }

        scatterQuery(terms, codes);
        TopScoreHeap hits = new TopScoreHeap(k);
        try {
            for (int internalId : internalIds) {
                hits.add(scoreLocatedDoc(internalId, locatorsOffset), internalId);
            }
        } finally {
            clearQuery(terms);
        }
        return toResults(hits, querySq);
    }

    /** The raw dot product of one document's stored vector against the dense query. */
    private float scoreLocatedDoc(int internalId, long locatorsOffset) throws IOException {
        input.seek(locatorsOffset + (long) internalId * NsparseFormat.DOC_LOCATOR_BYTES);
        long postingList = Integer.toUnsignedLong(input.readInt());
        int block = input.readInt();
        int slot = input.readInt();

        if (postingList == NsparseFormat.DOC_LOCATOR_REMAINDER) {
            return scoreRemainderRow(block, internalId);
        }
        if (postingList > Integer.MAX_VALUE) {
            throw corruptLocator(internalId);
        }
        int entry = reader.forwardIndex().entryOf((int) postingList, block);
        if (entry < 0) {
            throw corruptLocator(internalId);
        }
        InlineForwardIndexReader.InlineBlockView view = reader.forwardIndex().readBlock(input, entry, blockScratch);
        if (slot < 0 || slot >= view.numDocs() || view.docId(slot) != internalId) {
            throw corruptLocator(internalId);
        }
        return view.dotProduct(slot, dense);
    }

    /** The same, for a document that is in no block and whose vector lives in the remainder store. */
    private float scoreRemainderRow(int row, int internalId) throws IOException {
        DiskSeismicSqIndexReader.RemainderVectors remainder = reader.remainder();
        if (row < 0 || row >= remainder.count()) {
            throw corruptLocator(internalId);
        }
        input.seek(remainder.indptrOffset() + (long) row * Long.BYTES);
        long start = input.readLong();
        long end = input.readLong();
        if (start < 0 || end < start) {
            throw corruptLocator(internalId);
        }
        int nnz = Math.toIntExact(end - start);
        byte[] components = componentScratch.grow(nnz * NsparseFormat.INLINE_COMP_BYTES);
        input.seek(remainder.indicesOffset() + start * NsparseFormat.INLINE_COMP_BYTES);
        input.readBytes(components, 0, nnz * NsparseFormat.INLINE_COMP_BYTES);
        byte[] values = valueScratch.grow(nnz * elementSize);
        input.seek(remainder.valuesOffset() + start * elementSize);
        input.readBytes(values, 0, nnz * elementSize);

        float score = 0.0f;
        for (int i = 0; i < nnz; ++i) {
            int component = NsparseCodes.unsignedShortAt(components, i * NsparseFormat.INLINE_COMP_BYTES);
            score += NsparseCodes.codeAt(values, i * elementSize, elementSize) * NsparseCodes.codeAt(
                dense,
                component * elementSize,
                elementSize
            );
        }
        return score;
    }

    /**
     * The {@code cut} heaviest query tokens, which are the posting lists whose summaries are scored.
     * A cut at or above the query length is every token, unsorted, as in {@code top_k_tokens}.
     */
    private int[] cutTerms(int[] terms, int[] codes, int cut) {
        if (cut >= terms.length) {
            return terms;
        }
        if (cut <= 0) {
            return new int[0];
        }
        TopScoreHeap heaviest = new TopScoreHeap(cut);
        for (int i = 0; i < terms.length; ++i) {
            heaviest.add(codes[i], terms[i]);
        }
        float[] scores = new float[cut];
        int[] selected = new int[cut];
        int count = heaviest.drainDescending(scores, selected);
        return count == cut ? selected : ArrayUtil.copyOfSubArray(selected, 0, count);
    }

    /**
     * Every block of every cut token's posting list, with its summary score, into
     * {@link #candidateScores}/{@link #candidateBlocks}. Summary scores are comparable across posting
     * lists -- each is the query dotted with a cluster summary at the stored width -- which is what
     * lets the budget below be global rather than per list.
     *
     * @return how many candidates were collected
     */
    private int collectCandidates(int[] cutTerms, int[] terms, int[] codes) throws IOException {
        // The records are on disk, so ask for all of them before reading any: cut is small, and the
        // reads are far apart in the file.
        for (int term : cutTerms) {
            long offset = reader.summaryOffset(term);
            if (offset >= 0 && reader.summaryClusters(term) > 0) {
                input.prefetch(offset, summaryPrefetchLength(term));
            }
        }
        int count = 0;
        for (int term : cutTerms) {
            long offset = reader.summaryOffset(term);
            if (offset < 0 || reader.summaryClusters(term) == 0) {
                continue;
            }
            cursor.seek(offset);
            summaries.read(cursor, elementSize);
            int numClusters = summaries.numClusters();
            if (numClusters == 0) {
                continue;
            }
            if (summaryScores.length < numClusters) {
                summaryScores = new float[ArrayUtil.oversize(numClusters, Float.BYTES)];
            }
            summaries.scoreSummaries(terms, codes, summaryScores);
            if (candidateScores.length < count + numClusters) {
                int size = ArrayUtil.oversize(count + numClusters, Float.BYTES);
                candidateScores = ArrayUtil.growExact(candidateScores, size);
                candidateBlocks = ArrayUtil.growExact(candidateBlocks, size);
            }
            for (int cluster = 0; cluster < numClusters; ++cluster) {
                candidateScores[count] = summaryScores[cluster];
                candidateBlocks[count] = term << 16 | cluster;
                ++count;
            }
        }
        return count;
    }

    /**
     * How much of a summary record to prefetch. The record's length is not in an index, so this is the
     * distance to the next list's record -- exact for every list but the last, where the inline-forward
     * section's start bounds it.
     */
    private long summaryPrefetchLength(int term) {
        long start = reader.summaryOffset(term);
        long next = reader.summaryOffset(term + 1);
        long end = next > start ? next : reader.summaryEnd();
        return Math.max(1, end - start);
    }

    /**
     * The {@code k'} highest-scoring candidate blocks, packed as {@code (posting list, cluster)}.
     * Their order does not matter: dedup and exact scoring are order-independent.
     */
    private int[] selectBudget(int candidateCount, int kPrime) {
        int budget = Math.min(kPrime, candidateCount);
        if (budget == candidateCount) {
            return ArrayUtil.copyOfSubArray(candidateBlocks, 0, candidateCount);
        }
        TopScoreHeap best = new TopScoreHeap(budget);
        for (int i = 0; i < candidateCount; ++i) {
            best.add(candidateScores[i], candidateBlocks[i]);
        }
        float[] scores = new float[budget];
        int[] selected = new int[budget];
        int count = best.drainDescending(scores, selected);
        return count == budget ? selected : ArrayUtil.copyOfSubArray(selected, 0, count);
    }

    /** Scatters the query's codes into the dense lookup table, {@code elementSize} bytes each. */
    private void scatterQuery(int[] terms, int[] codes) {
        for (int i = 0; i < terms.length; ++i) {
            int offset = terms[i] * elementSize;
            dense[offset] = (byte) codes[i];
            if (elementSize == 2) {
                dense[offset + 1] = (byte) (codes[i] >>> 8);
            }
        }
    }

    /** Restores only the positions {@link #scatterQuery} touched, so the table stays reusable. */
    private void clearQuery(int[] terms) {
        for (int term : terms) {
            int offset = term * elementSize;
            dense[offset] = 0;
            if (elementSize == 2) {
                dense[offset + 1] = 0;
            }
        }
    }

    /**
     * Drains the hit heap into results: raw integer dot products decoded back to the float scale, and
     * internal ids mapped to Lucene doc ids.
     */
    private SparseQueryResult[] toResults(TopScoreHeap hits, NsparseScalarQuantizer querySq) {
        int count = hits.size();
        if (count == 0) {
            return NO_HITS;
        }
        float[] scores = new float[count];
        int[] internalIds = new int[count];
        hits.drainDescending(scores, internalIds);
        SparseQueryResult[] results = new SparseQueryResult[count];
        int written = 0;
        for (int i = 0; i < count; ++i) {
            int docId = reader.toDocId(internalIds[i]);
            if (docId < 0) {
                // Only reachable from a doc id the file's own structures disagree about; dropping the
                // hit is better than emitting -1, which a collector would read as a document.
                continue;
            }
            results[written++] = new SparseQueryResult(docId, reader.quantizer().decodeDotProduct(scores[i], querySq));
        }
        return written == count ? results : ArrayUtil.copyOfSubArray(results, 0, written);
    }

    /**
     * The filter's Lucene doc ids as this index's internal ids, ascending, dropping any document the
     * segment's index does not hold. Ascending so membership is a binary search.
     */
    private int[] toInternalIds(long[] docIds) {
        int[] internalIds = new int[docIds.length];
        int count = 0;
        for (long docId : docIds) {
            int internalId = docId >= 0 && docId <= Integer.MAX_VALUE ? reader.toInternalId((int) docId) : -1;
            if (internalId >= 0) {
                internalIds[count++] = internalId;
            }
        }
        internalIds = count == internalIds.length ? internalIds : ArrayUtil.copyOfSubArray(internalIds, 0, count);
        // The doc ids arrive ascending and the id map is ascending, so this is already sorted; sorting
        // makes the binary search above independent of that rather than reliant on it.
        Arrays.sort(internalIds);
        return internalIds;
    }

    private IOException corruptLocator(int internalId) {
        return new IOException("Native sparse index doc locator for internal id " + internalId + " does not resolve to its document");
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.DefaultNativeIndexWriter;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.query.DiskSeismicSqScorer;
import org.opensearch.neuralsearch.sparse.query.NativeIndexScorer;
import org.opensearch.neuralsearch.sparse.query.SparseQueryContext;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * Reads a {@code per_block} engine file built by the production writer through the Lucene-side reader,
 * and holds it against the native path over the same file.
 *
 * <p>The index is built by {@link DefaultNativeIndexWriter} rather than by a hand-written fixture,
 * because what is being tested is that this reader agrees with the C++ writer's layout -- a fixture
 * would only test it against itself. It is deliberately over the approximate threshold and
 * {@code per_block}, so the file holds an nsparse {@code disk_seismic_sq} index, and large enough that
 * posting lists are pruned and blocks hold many documents.
 */
public class DiskSeismicSqReadPathTests extends AbstractSparseTestBase {

    private static final String FIELD = "test_field";
    private static final int DOC_COUNT = 600;
    private static final int VOCABULARY = 64;
    private static final int TOKENS_PER_DOC = 6;
    /** Above the candidate pool of any query below, so no block budget is applied. */
    private static final float HEAP_FACTOR_NO_BUDGET = 20.0f;

    private Directory directory;
    private SegmentInfo segmentInfo;
    private FieldInfo fieldInfo;
    private LeafReader leafReader;
    private List<Map<Integer, Float>> vectors;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        directory = FSDirectory.open(createTempDir());
        segmentInfo = segmentInfo(DOC_COUNT);
        fieldInfo = perBlockFieldInfo();
        vectors = randomVectors(DOC_COUNT);
        writeIndex(fieldInfo, segmentInfo, vectors);
        leafReader = mock(LeafReader.class);
        when(leafReader.getCoreCacheHelper()).thenReturn(null);
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        directory.close();
        super.tearDown();
    }

    @SneakyThrows
    public void testParsesTheHeaderTheWriterWrote() {
        try (DiskSeismicSqIndexReader reader = openReader()) {
            assertFalse("a per_block index over the threshold has blocks to score", reader.isEmpty());
            assertEquals("every document reaches the index", DOC_COUNT, reader.getNumVectors());
            assertEquals("dimension is the highest token id plus one", VOCABULARY, reader.getDimension());
            assertEquals("the writer quantizes to 8-bit codes", 1, reader.quantizer().bytesPerValue());
            assertEquals("one posting list per token", VOCABULARY, reader.numPostingLists());
            assertTrue("the block directory is on the heap", reader.ramBytesUsed() > 0);
        }
    }

    /**
     * The reader must not hold the cluster summaries: they are read per query for the {@code cut} lists
     * a query names. Held, they are 4.3 GiB on MS MARCO at {@code lambda=6000, beta=400} -- which
     * OOM-killed a node with a 2 GiB heap on the first query, at a scale no unit test reaches. This
     * bounds the heap per document instead, which is the property that keeps it from happening.
     */
    @SneakyThrows
    public void testHeapDoesNotHoldTheSummaries() {
        try (DiskSeismicSqIndexReader reader = openReader()) {
            long bytes = reader.ramBytesUsed();
            long engineFileBytes = directory.fileLength(engineFileName());

            assertTrue(
                "reader heap " + bytes + " should be a small fraction of the " + engineFileBytes + " byte engine file",
                bytes < engineFileBytes / 4
            );
            assertTrue("reader heap " + bytes + " should be bounded per document", bytes < 512L * DOC_COUNT);
        }
    }

    /**
     * One searcher serves many queries, reusing the summary and block buffers between them. A buffer
     * left holding the previous query's list -- a stale length, an unreset offset -- would score the
     * wrong summaries, so every query through a reused searcher has to match a fresh one.
     */
    @SneakyThrows
    public void testReusedSearcherMatchesAFreshOneQueryAfterQuery() {
        List<Map<Integer, Float>> queries = List.of(
            queryTokens(),
            Map.of(3, 1.5f),
            Map.of(VOCABULARY - 1, 2.0f, 7, 0.25f),
            Map.of(11, 0.75f, 17, 2.25f, 29, 1.0f),
            queryTokens()
        );
        try (DiskSeismicSqIndexReader reader = openReader()) {
            DiskSeismicSqSearcher reused = new DiskSeismicSqSearcher(reader);
            for (Map<Integer, Float> tokens : queries) {
                SparseQueryResult[] fromReused = reused.search(query(tokens, 10, 1.0f, null));
                SparseQueryResult[] fromFresh = new DiskSeismicSqSearcher(reader).search(query(tokens, 10, 1.0f, null));

                assertEquals("hit count for " + tokens, fromFresh.length, fromReused.length);
                for (int i = 0; i < fromFresh.length; ++i) {
                    assertEquals("doc for " + tokens, fromFresh[i].getId(), fromReused[i].getId());
                    assertEquals("score for " + tokens, fromFresh[i].getScore(), fromReused[i].getScore(), 0.0f);
                }
            }
        }
    }

    /** The id map has to round-trip both ways, since a filtered query looks up by doc id. */
    @SneakyThrows
    public void testIdMapRoundTrips() {
        try (DiskSeismicSqIndexReader reader = openReader()) {
            for (int docId = 0; docId < DOC_COUNT; ++docId) {
                int internalId = reader.toInternalId(docId);
                assertTrue("every document is in the index", internalId >= 0);
                assertEquals(docId, reader.toDocId(internalId));
            }
            assertEquals("a doc id past the segment is not in the index", -1, reader.toInternalId(DOC_COUNT));
        }
    }

    /**
     * The whole point: the same query over the same file gives the same answer whether nsparse maps it
     * or this reader parses it. Run with no block budget, so both sides score every candidate block and
     * the comparison is of the arithmetic rather than of which blocks a budget happened to pick.
     */
    @SneakyThrows
    public void testAgreesWithTheNativePathWhenEveryBlockIsScored() {
        assertSameHits(queryTokens(), 25, HEAP_FACTOR_NO_BUDGET, null, null);
    }

    /** And with the budget actually applied, which is what a real query does. */
    @SneakyThrows
    public void testAgreesWithTheNativePathUnderABlockBudget() {
        assertSameHits(queryTokens(), 10, 1.0f, null, null);
    }

    /** A single-token query exercises one posting list's summaries and its blocks only. */
    @SneakyThrows
    public void testAgreesWithTheNativePathForASingleToken() {
        assertSameHits(Map.of(3, 1.5f), 10, HEAP_FACTOR_NO_BUDGET, null, null);
    }

    /**
     * A filter of at most k members makes both sides exact over exactly those documents, which is the
     * path through the doc-locator table and the remainder store -- the parts of the layout the
     * block-budget search never touches.
     */
    @SneakyThrows
    public void testAgreesWithTheNativePathForASmallFilter() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);
        for (int doc = 5; doc < 5 + 8; ++doc) {
            filter.set(doc);
        }

        List<Integer> docs = assertSameHits(queryTokens(), 8, 1.0f, null, new BitSetIterator(filter, filter.cardinality()));

        assertEquals("an exact match over the filter returns every member", 8, docs.size());
    }

    /** A filter larger than k stays approximate, restricted to the filter's documents. */
    @SneakyThrows
    public void testAgreesWithTheNativePathForALargeFilter() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);
        for (int doc = 0; doc < DOC_COUNT; doc += 3) {
            filter.set(doc);
        }

        List<Integer> docs = assertSameHits(
            queryTokens(),
            10,
            HEAP_FACTOR_NO_BUDGET,
            null,
            new BitSetIterator(filter, filter.cardinality())
        );

        for (int doc : docs) {
            assertTrue("only filtered documents can be hits: " + doc, filter.get(doc));
        }
    }

    @SneakyThrows
    public void testDeletedDocsAreDropped() {
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0, DOC_COUNT);
        List<Integer> deleted = new ArrayList<>();
        try (DiskSeismicSqIndexReader reader = openReader()) {
            DiskSeismicSqSearcher searcher = new DiskSeismicSqSearcher(reader);
            SparseQueryResult[] hits = searcher.search(query(queryTokens(), 3, HEAP_FACTOR_NO_BUDGET, null));
            for (SparseQueryResult hit : hits) {
                live.clear(hit.getId());
                deleted.add(hit.getId());
            }
        }

        List<Integer> docs = drain(scorer(queryTokens(), 3, HEAP_FACTOR_NO_BUDGET, live, null));

        assertEquals("k is still filled after the top hits are deleted", 3, docs.size());
        for (int doc : deleted) {
            assertFalse("a deleted document must not be returned: " + doc, docs.contains(doc));
        }
    }

    /** A segment where no document has the field gets a footer-only file, which must parse to empty. */
    @SneakyThrows
    public void testEmptySegmentParsesToAnEmptyIndex() {
        Directory emptyDirectory = FSDirectory.open(createTempDir());
        try {
            SegmentInfo info = segmentInfo(emptyDirectory, 0);
            FieldInfo field = perBlockFieldInfo();
            new DefaultNativeIndexWriter(writeState(emptyDirectory, info, field), field).writeIndex(docValues(List.of()));
            String fileName = CodecUtils.buildIndexFileName(
                info.name,
                SparseEngine.NATIVE.version(),
                FIELD,
                SparseEngine.NATIVE.extension()
            );

            try (DiskSeismicSqIndexReader reader = DiskSeismicSqIndexReader.open(emptyDirectory, fileName)) {
                assertTrue("nothing to score", reader.isEmpty());
                assertEquals(0, reader.getNumVectors());
                assertEquals(
                    "a searcher over it returns no hits",
                    0,
                    new DiskSeismicSqSearcher(reader).search(query(queryTokens(), 5, 1.0f, null)).length
                );
            }
        } finally {
            emptyDirectory.close();
        }
    }

    /** A file of a type this reader does not parse has to say so, not read the payload as a DSSQ one. */
    @SneakyThrows
    public void testRejectsAnIndexOfAnotherType() {
        Directory sharedDirectory = FSDirectory.open(createTempDir());
        try {
            SegmentInfo info = segmentInfo(sharedDirectory, DOC_COUNT);
            FieldInfo field = perBlockFieldInfo();
            // shared, so the writer builds seismic_sq rather than disk_seismic_sq
            field.putAttribute(FORWARD_INDEX_FIELD, SparseForwardIndex.SHARED.getName());
            writeIndex(field, info, vectors, sharedDirectory);
            String fileName = CodecUtils.buildIndexFileName(
                info.name,
                SparseEngine.NATIVE.version(),
                FIELD,
                SparseEngine.NATIVE.extension()
            );

            Exception e = expectThrows(Exception.class, () -> DiskSeismicSqIndexReader.open(sharedDirectory, fileName).close());
            assertTrue("the type mismatch should name both types: " + e.getMessage(), e.getMessage().contains("DSSQ"));
        } finally {
            sharedDirectory.close();
        }
    }

    /** A token no component id can hold matches nothing, and must not be narrowed onto one that can. */
    @SneakyThrows
    public void testTokensOutsideTheComponentSpaceAreDropped() {
        try (DiskSeismicSqIndexReader reader = openReader()) {
            DiskSeismicSqSearcher searcher = new DiskSeismicSqSearcher(reader);

            SparseQueryResult[] withJunk = searcher.search(
                new DiskSeismicSqQuery(new int[] { 3, 1 << 17, -4 }, new float[] { 1.5f, 9.0f, 9.0f }, 10, 3, 1000, 0.0f, 16.0f, null)
            );
            SparseQueryResult[] withoutJunk = searcher.search(
                new DiskSeismicSqQuery(new int[] { 3 }, new float[] { 1.5f }, 10, 3, 1000, 0.0f, 16.0f, null)
            );

            assertEquals(withoutJunk.length, withJunk.length);
            for (int i = 0; i < withJunk.length; ++i) {
                assertEquals(withoutJunk[i].getId(), withJunk[i].getId());
                assertEquals(withoutJunk[i].getScore(), withJunk[i].getScore(), 0.0f);
            }
        }
    }

    @SneakyThrows
    public void testRejectsANonPositiveBlockBudget() {
        try (DiskSeismicSqIndexReader reader = openReader()) {
            DiskSeismicSqSearcher searcher = new DiskSeismicSqSearcher(reader);
            expectThrows(
                IllegalArgumentException.class,
                () -> searcher.search(new DiskSeismicSqQuery(new int[] { 3 }, new float[] { 1.0f }, 10, 1, 0, 0.0f, 16.0f, null))
            );
        }
    }

    // ---- helpers ----

    /**
     * Scores the query through both scorers and asserts they returned the same documents with the same
     * scores.
     *
     * @return the doc ids both returned
     */
    @SneakyThrows
    private List<Integer> assertSameHits(
        Map<Integer, Float> tokens,
        int k,
        float heapFactor,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator
    ) {
        // A BitSetIterator is consumed by whichever scorer reads it first, so each gets its own.
        FixedBitSet filter = null;
        if (filterBitSetIterator != null) {
            filter = new FixedBitSet(DOC_COUNT);
            for (int doc = filterBitSetIterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = filterBitSetIterator.nextDoc()) {
                filter.set(doc);
            }
        }
        Map<Integer, Float> nativeHits = scored(
            new NativeIndexScorer(
                fieldInfo,
                queryContext(tokens, k, heapFactor),
                tokens,
                leafReader,
                segmentInfo,
                acceptedDocs,
                iteratorOver(filter),
                1.0f
            )
        );
        Map<Integer, Float> javaHits = scored(
            new DiskSeismicSqScorer(
                fieldInfo,
                queryContext(tokens, k, heapFactor),
                tokens,
                leafReader,
                segmentInfo,
                acceptedDocs,
                iteratorOver(filter),
                1.0f
            )
        );

        assertFalse("the query should match something, or the comparison is vacuous", nativeHits.isEmpty());
        assertEquals("the same documents", nativeHits.keySet(), javaHits.keySet());
        for (Map.Entry<Integer, Float> hit : nativeHits.entrySet()) {
            assertEquals("score of doc " + hit.getKey(), hit.getValue(), javaHits.get(hit.getKey()), 1e-4f);
        }
        return List.copyOf(nativeHits.keySet());
    }

    private BitSetIterator iteratorOver(FixedBitSet filter) {
        return filter == null ? null : new BitSetIterator(filter, filter.cardinality());
    }

    @SneakyThrows
    private Map<Integer, Float> scored(Scorer scorer) {
        Map<Integer, Float> hits = new TreeMap<>();
        DocIdSetIterator iterator = scorer.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            hits.put(doc, scorer.score());
        }
        return hits;
    }

    @SneakyThrows
    private List<Integer> drain(Scorer scorer) {
        return List.copyOf(scored(scorer).keySet());
    }

    @SneakyThrows
    private DiskSeismicSqScorer scorer(Map<Integer, Float> tokens, int k, float heapFactor, Bits acceptedDocs, BitSetIterator filter) {
        return new DiskSeismicSqScorer(
            fieldInfo,
            queryContext(tokens, k, heapFactor),
            tokens,
            leafReader,
            segmentInfo,
            acceptedDocs,
            filter,
            1.0f
        );
    }

    private SparseQueryContext queryContext(Map<Integer, Float> tokens, int k, float heapFactor) {
        return SparseQueryContext.builder()
            .tokens(tokens.keySet().stream().map(String::valueOf).toList())
            .heapFactor(heapFactor)
            .k(k)
            .build();
    }

    private DiskSeismicSqQuery query(Map<Integer, Float> tokens, int k, float heapFactor, long[] filterDocIds) {
        int[] terms = new int[tokens.size()];
        float[] weights = new float[tokens.size()];
        int i = 0;
        for (Map.Entry<Integer, Float> entry : tokens.entrySet()) {
            terms[i] = entry.getKey();
            weights[i] = entry.getValue();
            ++i;
        }
        int kPrime = Math.max(1, Math.round(20 + 100 * (heapFactor - 1.0f)));
        return new DiskSeismicSqQuery(terms, weights, k, terms.length, kPrime, 0.0f, 16.0f, filterDocIds);
    }

    private Map<Integer, Float> queryTokens() {
        Map<Integer, Float> tokens = new LinkedHashMap<>();
        tokens.put(3, 1.5f);
        tokens.put(11, 0.75f);
        tokens.put(17, 2.25f);
        tokens.put(29, 1.0f);
        tokens.put(41, 0.5f);
        return tokens;
    }

    @SneakyThrows
    private DiskSeismicSqIndexReader openReader() {
        return DiskSeismicSqIndexReader.open(directory, engineFileName());
    }

    /** Deterministic, so a parity failure is reproducible rather than seed-dependent. */
    private List<Map<Integer, Float>> randomVectors(int docCount) {
        Random random = new Random(0xC0FFEEL);
        List<Map<Integer, Float>> documents = new ArrayList<>(docCount);
        for (int doc = 0; doc < docCount; ++doc) {
            Map<Integer, Float> vector = new LinkedHashMap<>();
            while (vector.size() < TOKENS_PER_DOC) {
                vector.put(random.nextInt(VOCABULARY - 1), 0.05f + random.nextFloat() * 2.9f);
            }
            // Pins the dimension at VOCABULARY, so the assertion on it does not depend on the draw.
            vector.put(VOCABULARY - 1, 0.05f + random.nextFloat() * 2.9f);
            documents.add(vector);
        }
        return documents;
    }

    @SneakyThrows
    private void writeIndex(FieldInfo field, SegmentInfo info, List<Map<Integer, Float>> documents) {
        writeIndex(field, info, documents, directory);
    }

    @SneakyThrows
    private void writeIndex(FieldInfo field, SegmentInfo info, List<Map<Integer, Float>> documents, Directory dir) {
        new DefaultNativeIndexWriter(writeState(dir, info, field), field).writeIndex(docValues(documents));
        info.setFiles(
            Set.of(CodecUtils.buildIndexFileName(info.name, SparseEngine.NATIVE.version(), FIELD, SparseEngine.NATIVE.extension()))
        );
    }

    private String engineFileName() {
        return CodecUtils.buildIndexFileName(segmentInfo.name, SparseEngine.NATIVE.version(), FIELD, SparseEngine.NATIVE.extension());
    }

    private SegmentWriteState writeState(Directory dir, SegmentInfo info, FieldInfo field) {
        return new SegmentWriteState(
            InfoStream.getDefault(),
            dir,
            info,
            new FieldInfos(new FieldInfo[] { field }),
            null,
            IOContext.DEFAULT
        );
    }

    private SegmentInfo segmentInfo(int docCount) {
        return segmentInfo(directory, docCount);
    }

    private SegmentInfo segmentInfo(Directory dir, int docCount) {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            docCount,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }

    private FieldInfo perBlockFieldInfo() {
        FieldInfo info = new FieldInfo(
            FIELD,
            0,
            false,
            false,
            false,
            IndexOptions.DOCS,
            DocValuesType.BINARY,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        info.putAttribute(SPARSE_FIELD, "true");
        info.putAttribute(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        info.putAttribute(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName());
        // At or below the doc count, so the writer builds a quantized seismic index
        info.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "1");
        info.putAttribute(N_POSTINGS_FIELD, "120");
        info.putAttribute(CLUSTER_RATIO_FIELD, "0.1");
        info.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, "0.4");
        info.putAttribute(QUANTIZATION_CEILING_INGEST_FIELD, "3.0");
        info.putAttribute(QUANTIZATION_CEILING_SEARCH_FIELD, "16.0");
        return info;
    }

    private BinaryDocValues docValues(List<Map<Integer, Float>> documents) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return encode(documents.get(doc));
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                return doc < documents.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < documents.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return documents.size();
            }
        };
    }

    @SneakyThrows
    private BytesRef encode(Map<Integer, Float> weights) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            for (Map.Entry<Integer, Float> entry : weights.entrySet()) {
                dos.writeInt(entry.getKey());
                dos.writeFloat(entry.getValue());
            }
        }
        return new BytesRef(baos.toByteArray());
    }
}

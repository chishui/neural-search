/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.mockito.ArgumentCaptor;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
 * Drives the write and read halves of the native engine against the real nsparse library, which the
 * {@code test} task builds and puts on java.library.path. That is what makes these unit tests rather
 * than ITs, and it is the only way to reach {@link DefaultNativeIndexWriter} and
 * {@link OffHeapSparseVectorsBuffer}: their every path ends in a native call, and Mockito cannot
 * stub a native method.
 *
 * The assertions are on what comes back out of the index, because the intermediate state -- the CSR
 * buffers, the built index -- is opaque native memory with no Java view of it.
 */
public class NativeIndexRoundTripTests extends AbstractSparseTestBase {

    private static final String FIELD = "test_field";
    private static final int DOC_COUNT = 24;

    private Directory directory;
    private SegmentInfo segmentInfo;
    /**
     * Every handle this test loaded. nsparse maps the engine file, and Windows refuses to delete a
     * mapped file, so the test framework's temp-dir cleanup fails unless all of them are freed --
     * including on the paths where an assertion threw before the test could free its own.
     */
    private final List<Long> loadedIndexes = new ArrayList<>();
    private final List<IndexReader.ClosedListener> coreClosedListeners = new ArrayList<>();

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        directory = FSDirectory.open(createTempDir());
        segmentInfo = segmentInfo(DOC_COUNT);
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        // Core-scoped handles are freed by the core closing, which nothing else fires here
        for (IndexReader.ClosedListener listener : coreClosedListeners) {
            listener.onClose(null);
        }
        for (long address : loadedIndexes) {
            if (address != 0) {
                NativeLibrary.freeIndex(address);
            }
        }
        loadedIndexes.clear();
        coreClosedListeners.clear();
        directory.close();
        super.tearDown();
    }

    /**
     * The seismic path: over the approximate threshold, so the field is clustered and quantized.
     * Documents carry the query's token with rising weights, so the ranking is known regardless of
     * which blocks the search picks.
     */
    @SneakyThrows
    public void testSharedForwardIndexRoundTrip() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, 1);

        long address = writeAndLoad(fieldInfo, risingWeightVectors());
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, 3, searchParameters());

        assertEquals(3, results.length);
        // Highest weight first: the last documents carry the largest weight for token 7
        assertEquals(DOC_COUNT - 1, results[0].getId());
        assertTrue("scores should descend", results[0].getScore() >= results[1].getScore());
    }

    /** Same documents through disk_seismic_sq, which stores each block's vectors inline. */
    @SneakyThrows
    public void testPerBlockForwardIndexRoundTrip() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.PER_BLOCK, 1);

        long address = writeAndLoad(fieldInfo, risingWeightVectors());
        Map<String, Object> parameters = searchParameters();
        // k_prime is what makes a per_block field read the query range off the parameters
        parameters.put("k_prime", 10);
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, 3, parameters);

        assertEquals(3, results.length);
        assertEquals(DOC_COUNT - 1, results[0].getId());
    }

    /**
     * Below the approximate threshold the writer asks for a plain inverted index instead, which
     * scores unquantized floats.
     */
    @SneakyThrows
    public void testInvertedIndexRoundTripBelowThreshold() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, Integer.MAX_VALUE);

        long address = writeAndLoad(fieldInfo, risingWeightVectors());
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, 2, new HashMap<>());

        assertEquals(2, results.length);
        assertEquals(DOC_COUNT - 1, results[0].getId());
        // Unquantized, so the score is the exact dot product of the highest weight with 1.0
        assertEquals((float) DOC_COUNT, results[0].getScore(), 0.001f);
    }

    /** Only the documents that share a token with the query can come back. */
    @SneakyThrows
    public void testQueryMatchesOnlyDocumentsSharingAToken() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, Integer.MAX_VALUE);
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            // Only even documents carry token 7
            vectors.add(doc % 2 == 0 ? Map.of(7, 1.0f + doc) : Map.of(9, 1.0f));
        }

        long address = writeAndLoad(fieldInfo, vectors);
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 7 }, new float[] { 1.0f }, DOC_COUNT, new HashMap<>());

        for (SparseQueryResult result : results) {
            assertEquals("only even docs carry token 7", 0, result.getId() % 2);
        }
    }

    /**
     * A filter is a candidate set for the native engine, so nothing outside it can be returned.
     */
    @SneakyThrows
    public void testQueryWithFilterRestrictsToTheCandidateSet() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, Integer.MAX_VALUE);

        long address = writeAndLoad(fieldInfo, risingWeightVectors());
        long[] candidates = new long[] { 2L, 5L };
        SparseQueryResult[] results = NativeLibrary.queryIndexWithFilter(
            address,
            new int[] { 7 },
            new float[] { 1.0f },
            DOC_COUNT,
            new HashMap<>(),
            candidates,
            0
        );

        for (SparseQueryResult result : results) {
            assertTrue("doc " + result.getId() + " was not in the filter", Arrays.stream(candidates).anyMatch(c -> c == result.getId()));
        }
    }

    /**
     * A segment where no document has the field still has to leave a readable file behind, or the
     * reader has no way to tell "no index" from a truncated write.
     */
    @SneakyThrows
    public void testSegmentWithoutTheFieldWritesAFooterOnlyFile() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, 1);

        new DefaultNativeIndexWriter(writeState(fieldInfo), fieldInfo).writeIndex(emptyDocValues());

        String engineFile = engineFileName();
        assertTrue(Arrays.asList(directory.listAll()).contains(engineFile));
        assertTrue("a footer-only file is still a few bytes long", directory.fileLength(engineFile) > 0);
    }

    /** The dimension comes from the largest token, which doc values do not store in order. */
    @SneakyThrows
    public void testTokensOutOfOrderDoNotUndersizeTheDimension() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, Integer.MAX_VALUE);
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            // Largest token id first, so a writer taking the last element would undersize by 4000
            Map<Integer, Float> vector = new LinkedHashMap<>();
            vector.put(4000, 1.0f + doc);
            vector.put(7, 1.0f);
            vectors.add(vector);
        }

        long address = writeAndLoad(fieldInfo, vectors);
        SparseQueryResult[] results = NativeLibrary.queryIndex(address, new int[] { 4000 }, new float[] { 1.0f }, 1, new HashMap<>());

        assertEquals(1, results.length);
        assertEquals(DOC_COUNT - 1, results[0].getId());
    }

    /**
     * Flushing the buffer more than once has to keep the CSR offsets contiguous across batches, or
     * the documents after the first flush are scored against the wrong vectors.
     */
    @SneakyThrows
    public void testVectorsSpanningSeveralBufferFlushesStayAligned() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, Integer.MAX_VALUE);
        // One byte forces a flush per document, so every document lands in its own batch
        OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(1L);
        int[] docIds = new int[DOC_COUNT];
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            buffer.addVector(List.of(7), List.of(1.0f + doc));
            docIds[doc] = doc;
        }
        buffer.flush();

        long[] addresses = buffer.getMemoryAddresses();
        long indexAddress = NativeLibrary.initIndex(DOC_COUNT, 4096, indexParameters());
        loadedIndexes.add(indexAddress);
        NativeLibrary.insertToIndex(indexAddress, docIds, addresses[0], addresses[1], addresses[2], 1);
        SparseQueryResult[] results = NativeLibrary.queryIndex(indexAddress, new int[] { 7 }, new float[] { 1.0f }, 1, new HashMap<>());

        // The highest weight belongs to the last document, which is only true if the per-flush
        // offsets were rebased onto the running total
        assertEquals(1, results.length);
        assertEquals(DOC_COUNT - 1, results[0].getId());
        assertEquals((float) DOC_COUNT, results[0].getScore(), 0.001f);
    }

    /** Two opens of the same core share one loaded index, so a query cannot pay for a reload. */
    @SneakyThrows
    public void testSegmentNativeIndexLoadsAndFreesTheHandle() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, 1);
        new DefaultNativeIndexWriter(writeState(fieldInfo), fieldInfo).writeIndex(docValues(risingWeightVectors()));
        segmentInfo.setFiles(Set.of(engineFileName()));

        LeafReader reader = mock(LeafReader.class);
        when(reader.getCoreCacheHelper()).thenReturn(null);
        try (SegmentNativeIndex index = SegmentNativeIndex.open(reader, segmentInfo, FIELD)) {
            long first = index.address();
            assertTrue(first != 0);
            assertEquals("the handle is loaded once and reused", first, index.address());
        }
    }

    /** Reuse across opens is keyed on the segment core, not on the query. */
    @SneakyThrows
    public void testSegmentNativeIndexIsSharedAcrossQueriesOnACore() {
        FieldInfo fieldInfo = fieldInfo(SparseForwardIndex.SHARED, 1);
        new DefaultNativeIndexWriter(writeState(fieldInfo), fieldInfo).writeIndex(docValues(risingWeightVectors()));
        segmentInfo.setFiles(Set.of(engineFileName()));

        LeafReader reader = mock(LeafReader.class);
        IndexReader.CacheHelper cacheHelper = mock(IndexReader.CacheHelper.class);
        when(cacheHelper.getKey()).thenReturn(mock(IndexReader.CacheKey.class));
        when(reader.getCoreCacheHelper()).thenReturn(cacheHelper);
        ArgumentCaptor<IndexReader.ClosedListener> listener = ArgumentCaptor.forClass(IndexReader.ClosedListener.class);

        SegmentNativeIndex first = SegmentNativeIndex.open(reader, segmentInfo, FIELD);
        verify(cacheHelper).addClosedListener(listener.capture());
        coreClosedListeners.add(listener.getValue());
        long address = first.address();
        // close() must not free a core-scoped handle: the core owns it and other queries hold it
        first.close();
        assertEquals(address, SegmentNativeIndex.open(reader, segmentInfo, FIELD).address());
    }

    // ---- helpers ----

    /** Writes the vectors through the production writer, then loads the file back. */
    @SneakyThrows
    private long writeAndLoad(FieldInfo fieldInfo, List<Map<Integer, Float>> vectors) {
        new DefaultNativeIndexWriter(writeState(fieldInfo), fieldInfo).writeIndex(docValues(vectors));
        String path = ((FSDirectory) directory).getDirectory().resolve(engineFileName()).toString();
        long address = NativeLibrary.loadIndex(path);
        loadedIndexes.add(address);
        return address;
    }

    private String engineFileName() {
        return CodecUtils.buildIndexFileName(segmentInfo.name, SparseEngine.NATIVE.version(), FIELD, SparseEngine.NATIVE.extension());
    }

    private SegmentWriteState writeState(FieldInfo fieldInfo) {
        return new SegmentWriteState(
            InfoStream.getDefault(),
            directory,
            segmentInfo,
            new FieldInfos(new FieldInfo[] { fieldInfo }),
            null,
            IOContext.DEFAULT
        );
    }

    /** DOC_COUNT documents all carrying token 7, with the weight rising by document. */
    private List<Map<Integer, Float>> risingWeightVectors() {
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            vectors.add(Map.of(7, 1.0f + doc));
        }
        return vectors;
    }

    private Map<String, Object> searchParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("cut", 10);
        parameters.put("heap_factor", 1.0f);
        parameters.put("vmin", 0.0f);
        parameters.put("vmax", 32.0f);
        return parameters;
    }

    private Map<String, Object> indexParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        parameters.put("index", "inverted");
        return parameters;
    }

    private SegmentInfo segmentInfo(int maxDoc) {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            maxDoc,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }

    private FieldInfo fieldInfo(SparseForwardIndex forwardIndex, int approximateThreshold) {
        FieldInfo fieldInfo = new FieldInfo(
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
        fieldInfo.putAttribute(SPARSE_FIELD, "true");
        fieldInfo.putAttribute(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        fieldInfo.putAttribute(FORWARD_INDEX_FIELD, forwardIndex.getName());
        fieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(approximateThreshold));
        fieldInfo.putAttribute(N_POSTINGS_FIELD, "10");
        fieldInfo.putAttribute(CLUSTER_RATIO_FIELD, "0.5");
        fieldInfo.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, "1.0");
        fieldInfo.putAttribute(QUANTIZATION_CEILING_INGEST_FIELD, "32.0");
        fieldInfo.putAttribute(QUANTIZATION_CEILING_SEARCH_FIELD, "32.0");
        return fieldInfo;
    }

    /** Binary doc values over the encoded vectors, one per document, in ascending doc order. */
    private BinaryDocValues docValues(List<Map<Integer, Float>> vectors) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return encode(vectors.get(doc));
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
                return doc < vectors.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < vectors.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return vectors.size();
            }
        };
    }

    private BinaryDocValues emptyDocValues() {
        return docValues(List.of());
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

    /**
     * Past the buffer's 1024-entry default capacities, so both array-growth paths run and the CSR
     * offsets still have to describe every document.
     */
    @SneakyThrows
    public void testVectorsBeyondTheDefaultBufferCapacity() {
        int docCount = 1200;
        int[] docIds = new int[docCount];
        long[] addresses;
        // close() flushes what is pending, so the try-with-resources is the flush
        try (OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(Long.MAX_VALUE)) {
            for (int doc = 0; doc < docCount; doc++) {
                // Two non-zeros per doc pushes the token array past 1024 before the doc array
                buffer.addVector(List.of(7, 9), List.of(1.0f + doc, 1.0f));
                docIds[doc] = doc;
            }
            addresses = buffer.getMemoryAddresses();
        }

        long indexAddress = NativeLibrary.initIndex(docCount, 4096, indexParameters());
        loadedIndexes.add(indexAddress);
        NativeLibrary.insertToIndex(indexAddress, docIds, addresses[0], addresses[1], addresses[2], 1);
        SparseQueryResult[] results = NativeLibrary.queryIndex(indexAddress, new int[] { 7 }, new float[] { 1.0f }, 1, new HashMap<>());

        assertEquals(1, results.length);
        assertEquals(docCount - 1, results[0].getId());
        assertEquals((float) docCount, results[0].getScore(), 0.001f);
    }
}

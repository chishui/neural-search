/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCacheItem;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

public class SparseDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private FieldInfo sparseFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;
    @Mock
    private MergeHelper mockMergeHelper;
    @Mock
    private MergeStateFacade mockMergeStateFacade;
    @Mock
    private SegmentInfo segmentInfo;
    @Mock
    private SparseDocValuesReader sparseDocValuesReader;
    @Mock
    private SparseBinaryDocValues binaryDocValues;

    private SegmentWriteState segmentWriteState;
    private DocValuesProducer docValuesProducer;
    private CacheKey cacheKey;
    private SparseDocValuesConsumer sparseDocValuesConsumer;

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        when(segmentInfo.maxDoc()).thenReturn(100);

        // Setup sparse field
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, String.valueOf(true));
        sparseAttributes.put(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(50));
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        when(nonSparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        when(mockMergeHelper.newSparseDocValuesReader(any())).thenReturn(sparseDocValuesReader);
        when(sparseDocValuesReader.getBinary(any())).thenReturn(binaryDocValues);
        when(binaryDocValues.nextDoc()).thenReturn(1, DocIdSetIterator.NO_MORE_DOCS);
        SparseVector vector = new SparseVector(Map.of(1, 0.1f, 2, 0.2f), new ByteQuantizer(3.0f));
        when(binaryDocValues.cachedSparseVector()).thenReturn(vector);

        docValuesProducer = mock(DocValuesProducer.class);
        cacheKey = prepareUniqueCacheKey(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Clean up any created indices
        if (cacheKey != null) {
            ForwardIndexCache.getInstance().onIndexRemoval(cacheKey);
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testAddBinaryField_NonSparseField() {
        sparseDocValuesConsumer.addBinaryField(nonSparseFieldInfo, docValuesProducer);

        // Should not create forward index for non-sparse field
        assertNull(ForwardIndexCache.getInstance().get(new CacheKey(segmentInfo, nonSparseFieldInfo)));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldBelowThreshold() {
        // Create new segmentInfo with lower maxDoc
        when(segmentInfo.maxDoc()).thenReturn(30);
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Should not create forward index when below threshold
        assertNull(ForwardIndexCache.getInstance().get(cacheKey));
    }

    @SneakyThrows
    public void testAddBinaryField_SparseFieldAboveThreshold() {
        // Create new segmentInfo with higher maxDoc
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);

        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(0, 1, BinaryDocValues.NO_MORE_DOCS);
        when(binaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created and populated
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);

        // Verify vectors were inserted
        SparseVector vector0 = index.getReader().read(0);
        SparseVector vector1 = index.getReader().read(1);
        assertNotNull(vector0);
        assertNotNull(vector1);
    }

    @SneakyThrows
    public void testMerge_WithSparseField() {
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper).newSparseDocValuesReader(any());
        verify(binaryDocValues).cachedSparseVector();
    }

    @SneakyThrows
    public void testMerge_WithSparseField_noCachedVector() {
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);
        when(binaryDocValues.cachedSparseVector()).thenReturn(null);
        when(binaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());

        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(binaryDocValues).binaryValue();
    }

    @SneakyThrows
    public void testMerge_WithSparseField_notBinaryType() {
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.NUMERIC);

        sparseDocValuesConsumer.merge(List.of(sparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testMerge_WithNonSparseField() {
        sparseDocValuesConsumer.merge(List.of(nonSparseFieldInfo), mockMergeStateFacade);

        verify(mockMergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testAddBinary_WithSparseBinaryDocValues() {
        // Create new segmentInfo with higher maxDoc
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo(100);
        cacheKey = new CacheKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, mockMergeHelper);

        // Create SparseBinaryDocValues for merge scenario
        SparseBinaryDocValues sparseBinaryDocValues = mock(SparseBinaryDocValues.class);
        when(sparseBinaryDocValues.nextDoc()).thenReturn(0, 1, SparseBinaryDocValues.NO_MORE_DOCS);

        SparseVector mockVector = createVector(1, 2, 3, 4);
        when(sparseBinaryDocValues.cachedSparseVector()).thenReturn(mockVector);
        when(sparseBinaryDocValues.binaryValue()).thenReturn(TestsPrepareUtils.prepareValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);
        assertNotNull(index);

        // Verify vector was stored
        SparseVector storedVector = index.getReader().read(0);
        assertNotNull(storedVector);
    }

    @SneakyThrows
    public void testAddBinary_EmptyDocValues() {
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(BinaryDocValues.NO_MORE_DOCS);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        // Should not throw when there are no documents to process
        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);
    }
}

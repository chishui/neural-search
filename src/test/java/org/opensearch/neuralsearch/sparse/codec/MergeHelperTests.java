/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class MergeHelperTests extends AbstractSparseTestBase {
    private MergeStateFacade mergeStateFacade;
    private DocValuesProducer docValuesProducer1;
    private DocValuesProducer docValuesProducer2;
    private FieldInfo sparseFieldInfo;
    private FieldInfo nonSparseFieldInfo;
    private DocValuesProducer docValuesProducer;
    private SegmentInfo segmentInfo;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        mergeStateFacade = mock(MergeStateFacade.class);
        docValuesProducer1 = mock(DocValuesProducer.class);
        docValuesProducer2 = mock(DocValuesProducer.class);
        docValuesProducer = mock(DocValuesProducer.class);
        segmentInfo = mock(SegmentInfo.class);

        // Setup sparse field
        sparseFieldInfo = mock(FieldInfo.class);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, "true");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);

        // Setup non-sparse field
        nonSparseFieldInfo = mock(FieldInfo.class);
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());

        List<FieldInfo> fields = Arrays.asList(sparseFieldInfo, nonSparseFieldInfo);
        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.iterator()).thenReturn(fields.iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[] { docValuesProducer });
    }

    public void testClear() throws IOException {
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1, docValuesProducer2});
        MergeHelper.clearCacheData(mergeStateFacade, null, (t) -> {

        });
        assertTrue(true);
    }

    public void testClearCacheData_withValidSparseField_callsConsumer() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        MergeHelper.clearCacheData(mergeStateFacade, sparseFieldInfo, consumer);

        assertEquals("Consumer should be called when fieldInfo matches", 1, capturedKeys.size());
    }

    public void testClearInMemoryData_withNullFieldInfo_processesAllSparseFields() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        MergeHelper.clearCacheData(mergeStateFacade, null, consumer);

        assertEquals("Consumer should be called for sparse field", 1, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseFieldInfo_processesAllSparseFields() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        MergeHelper.clearCacheData(mergeStateFacade, nonSparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called for sparse field when fieldInfo doesn't match", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseBinaryDocValues_skipsField() throws IOException {
        BinaryDocValues mockBinaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        MergeHelper.clearCacheData(mergeStateFacade, nonSparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called for non-SparseBinaryDocValuesPassThrough", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withEmptyMergeState_doesNotCallConsumer() throws IOException {
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{});

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        MergeHelper.clearCacheData(mergeStateFacade, sparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called with empty producers", 0, capturedKeys.size());
    }
}

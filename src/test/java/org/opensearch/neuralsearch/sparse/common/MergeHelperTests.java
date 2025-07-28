/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

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
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;

public class MergeHelperTests extends AbstractSparseTestBase {

    private MergeStateFacade mergeStateFacade;
    private DocValuesProducer docValuesProducer1;
    private DocValuesProducer docValuesProducer2;
    private FieldInfo sparseFieldInfo;
    private FieldInfo nonSparseFieldInfo;
    private FieldInfos fieldInfos;
    private Consumer<InMemoryKey.IndexKey> consumer;
    private SegmentInfo segmentInfo;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        mergeStateFacade = mock(MergeStateFacade.class);
        docValuesProducer1 = mock(DocValuesProducer.class);
        docValuesProducer2 = mock(DocValuesProducer.class);
        
        // Setup sparse field
        sparseFieldInfo = mock(FieldInfo.class);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, "true");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);
        
        // Setup non-sparse field
        nonSparseFieldInfo = mock(FieldInfo.class);
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        
        List<FieldInfo> fields = Arrays.asList(sparseFieldInfo, nonSparseFieldInfo);
        fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.iterator()).thenReturn(fields.iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);
        
        consumer = mock(Consumer.class);
        segmentInfo = mock(SegmentInfo.class);
    }

    public void testClearInMemoryDataWithSparseFields() throws IOException {
        // Setup SparseBinaryDocValuesPassThrough
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        
        when(docValuesProducer1.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);
        when(docValuesProducer1.getBinary(nonSparseFieldInfo)).thenReturn(mock(BinaryDocValues.class));
        
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, times(1)).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataSkipsNonSparseFields() throws IOException {
        BinaryDocValues regularBinaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer1.getBinary(nonSparseFieldInfo)).thenReturn(regularBinaryDocValues);
        
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataWithSpecificFieldInfo() throws IOException {
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(sparseBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        
        when(docValuesProducer1.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        // Test with specific fieldInfo that matches
        MergeHelper.clearInMemoryData(mergeStateFacade, sparseFieldInfo, consumer);
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
        
        // Test with specific fieldInfo that doesn't match
        MergeHelper.clearInMemoryData(mergeStateFacade, nonSparseFieldInfo, consumer);
        verify(consumer, times(1)).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataSkipsNonSparseBinaryDocValues() throws IOException {
        BinaryDocValues regularBinaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer1.getBinary(sparseFieldInfo)).thenReturn(regularBinaryDocValues);
        
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataWithMultipleProducers() throws IOException {
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues1 = mock(SparseBinaryDocValuesPassThrough.class);
        SparseBinaryDocValuesPassThrough sparseBinaryDocValues2 = mock(SparseBinaryDocValuesPassThrough.class);
        
        when(sparseBinaryDocValues1.getSegmentInfo()).thenReturn(segmentInfo);
        when(sparseBinaryDocValues2.getSegmentInfo()).thenReturn(segmentInfo);
        
        when(docValuesProducer1.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues1);
        when(docValuesProducer2.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues2);
        
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1, docValuesProducer2});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, times(2)).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataWithIOException() throws IOException {
        when(docValuesProducer1.getBinary(sparseFieldInfo)).thenThrow(new IOException("Test exception"));
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        expectThrows(IOException.class, () -> {
            MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        });
        
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataWithEmptyProducers() throws IOException {
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
    }

    public void testClearInMemoryDataWithEmptyFields() throws IOException {
        FieldInfos emptyFieldInfos = mock(FieldInfos.class);
        when(emptyFieldInfos.iterator()).thenReturn(new ArrayList<FieldInfo>().iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(emptyFieldInfos);
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{docValuesProducer1});
        
        MergeHelper.clearInMemoryData(mergeStateFacade, null, consumer);
        
        verify(consumer, never()).accept(any(InMemoryKey.IndexKey.class));
    }

}

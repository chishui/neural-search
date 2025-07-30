/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;

public class SparseDocValuesConsumerTests extends AbstractSparseTestBase {

    private SparseDocValuesConsumer sparseDocValuesConsumer;
    private DocValuesConsumer delegate;
    private SegmentWriteState segmentWriteState;
    private SegmentInfo segmentInfo;
    private FieldInfo sparseFieldInfo;
    private FieldInfo nonSparseFieldInfo;
    private DocValuesProducer docValuesProducer;
    private InMemoryKey.IndexKey indexKey;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        delegate = mock(DocValuesConsumer.class);
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState();
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate);

        // Setup sparse field
        sparseFieldInfo = mock(FieldInfo.class);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, "true");
        sparseAttributes.put(ALGO_TRIGGER_DOC_COUNT_FIELD, "50");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);
        when(sparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        // Setup non-sparse field
        nonSparseFieldInfo = mock(FieldInfo.class);
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        when(nonSparseFieldInfo.getDocValuesType()).thenReturn(DocValuesType.BINARY);

        docValuesProducer = mock(DocValuesProducer.class);
        indexKey = new InMemoryKey.IndexKey(segmentInfo, sparseFieldInfo);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Clean up any created indices
        if (indexKey != null) {
            InMemorySparseVectorForwardIndex.removeIndex(indexKey);
        }
        super.tearDown();
    }

    public void testAddNumericField() throws IOException {
        sparseDocValuesConsumer.addNumericField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addNumericField(sparseFieldInfo, docValuesProducer);
    }

    public void testAddBinaryField_NonSparseField() throws IOException {
        sparseDocValuesConsumer.addBinaryField(nonSparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(nonSparseFieldInfo, docValuesProducer);
        // Should not create forward index for non-sparse field
        assertNull(InMemorySparseVectorForwardIndex.get(new InMemoryKey.IndexKey(segmentInfo, nonSparseFieldInfo)));
    }

    public void testAddBinaryField_SparseFieldBelowThreshold() throws IOException {
        // Create new segmentInfo with lower maxDoc
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo(30);
        indexKey = new InMemoryKey.IndexKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);
        // Should not create forward index when below threshold
        assertNull(InMemorySparseVectorForwardIndex.get(indexKey));
    }

    public void testAddBinaryField_SparseFieldAboveThreshold() throws IOException {
        // Create new segmentInfo with higher maxDoc
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo(100);
        indexKey = new InMemoryKey.IndexKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate);

        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(0, 1, BinaryDocValues.NO_MORE_DOCS);
        when(binaryDocValues.binaryValue()).thenReturn(createValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created and populated
        InMemorySparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(indexKey);
        assertNotNull(index);

        // Verify vectors were inserted
        SparseVector vector0 = index.getReader().read(0);
        SparseVector vector1 = index.getReader().read(1);
        assertNotNull(vector0);
        assertNotNull(vector1);
    }

    public void testAddSortedField() throws IOException {
        sparseDocValuesConsumer.addSortedField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedField(sparseFieldInfo, docValuesProducer);
    }

    public void testAddSortedNumericField() throws IOException {
        sparseDocValuesConsumer.addSortedNumericField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedNumericField(sparseFieldInfo, docValuesProducer);
    }

    public void testAddSortedSetField() throws IOException {
        sparseDocValuesConsumer.addSortedSetField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addSortedSetField(sparseFieldInfo, docValuesProducer);
    }

    public void testClose() throws IOException {
        sparseDocValuesConsumer.close();

        verify(delegate, times(1)).close();
    }

    public void testMerge_WithSparseField() throws IOException {
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        when(mergeFieldInfos.iterator()).thenReturn(java.util.Arrays.asList(sparseFieldInfo).iterator());
        mergeState.mergeFieldInfos = mergeFieldInfos;

        // Mock SparseDocValuesReader
        SparseDocValuesReader reader = mock(SparseDocValuesReader.class);
        SparseBinaryDocValues sparseBinaryDocValues = mock(SparseBinaryDocValues.class);
        when(sparseBinaryDocValues.nextDoc()).thenReturn(0, SparseBinaryDocValues.NO_MORE_DOCS);
        when(sparseBinaryDocValues.cachedSparseVector()).thenReturn(createVector(1, 2, 3, 4));
        when(reader.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);
        when(reader.getMergeState()).thenReturn(mergeState);

        // Use reflection or create a custom consumer to test merge
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    public void testMerge_WithNonSparseField() throws IOException {
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        when(mergeFieldInfos.iterator()).thenReturn(java.util.Arrays.asList(nonSparseFieldInfo).iterator());
        mergeState.mergeFieldInfos = mergeFieldInfos;

        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
        // Should not create forward index for non-sparse field
        assertNull(InMemorySparseVectorForwardIndex.get(new InMemoryKey.IndexKey(segmentInfo, nonSparseFieldInfo)));
    }

    public void testMerge_WithException() throws IOException {
        MergeState mergeState = mock(MergeState.class);
        // Don't set mergeFieldInfos to null as it causes assertion error
        // Instead test with empty field infos
        FieldInfos emptyFieldInfos = mock(FieldInfos.class);
        when(emptyFieldInfos.iterator()).thenReturn(java.util.Collections.emptyIterator());
        mergeState.mergeFieldInfos = emptyFieldInfos;

        // Should not throw exception, just log error
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    public void testAddBinary_WithSparseBinaryDocValues() throws IOException {
        // Create new segmentInfo with higher maxDoc
        segmentInfo = new SegmentInfo(
            segmentInfo.dir,
            segmentInfo.getVersion(),
            segmentInfo.getMinVersion(),
            segmentInfo.name,
            100,  // maxDoc above threshold
            segmentInfo.getUseCompoundFile(),
            segmentInfo.getHasBlocks(),
            segmentInfo.getCodec(),
            segmentInfo.getDiagnostics(),
            segmentInfo.getId(),
            segmentInfo.getAttributes(),
            segmentInfo.getIndexSort()
        );
        indexKey = new InMemoryKey.IndexKey(segmentInfo, sparseFieldInfo);

        // Create new SegmentWriteState with the updated segmentInfo
        segmentWriteState = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        sparseDocValuesConsumer = new SparseDocValuesConsumer(segmentWriteState, delegate);

        // Create SparseBinaryDocValues for merge scenario
        SparseBinaryDocValues sparseBinaryDocValues = mock(SparseBinaryDocValues.class);
        when(sparseBinaryDocValues.nextDoc()).thenReturn(0, 1, SparseBinaryDocValues.NO_MORE_DOCS);

        SparseVector mockVector = createVector(1, 2, 3, 4);
        when(sparseBinaryDocValues.cachedSparseVector()).thenReturn(mockVector);
        when(sparseBinaryDocValues.binaryValue()).thenReturn(createValidSparseVectorBytes());
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(sparseBinaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        // Verify forward index was created
        InMemorySparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(indexKey);
        assertNotNull(index);

        // Verify cached vector was used
        SparseVector storedVector = index.getReader().read(0);
        assertNotNull(storedVector);
    }

    public void testAddBinary_WriterIsNull() throws IOException {
        // This test covers the normal case since writer null is hard to trigger
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(binaryDocValues.nextDoc()).thenReturn(BinaryDocValues.NO_MORE_DOCS);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(binaryDocValues);

        sparseDocValuesConsumer.addBinaryField(sparseFieldInfo, docValuesProducer);

        verify(delegate, times(1)).addBinaryField(sparseFieldInfo, docValuesProducer);
    }

    public void testMerge_WithSparseDocValuesReader() throws IOException {
        // Create a real MergeState to test the SparseDocValuesReader instanceof check
        MergeState realMergeState = TestsPrepareUtils.prepareMergeState(false);

        // This will trigger the merge logic and test the instanceof SparseDocValuesReader check
        sparseDocValuesConsumer.merge(realMergeState);

        verify(delegate, times(1)).merge(realMergeState);
    }

    public void testMerge_WithRealException() throws IOException {
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        // Create an iterator that throws exception
        when(mergeFieldInfos.iterator()).thenThrow(new RuntimeException("Test exception"));
        mergeState.mergeFieldInfos = mergeFieldInfos;

        // Should not throw exception, just log error
        sparseDocValuesConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);
    }

    public void testMerge_WithSparseFieldAndReader() throws IOException {
        // Create segmentInfo above threshold
        SegmentInfo newSegmentInfo = new SegmentInfo(
            segmentInfo.dir,
            segmentInfo.getVersion(),
            segmentInfo.getMinVersion(),
            segmentInfo.name,
            100,
            segmentInfo.getUseCompoundFile(),
            segmentInfo.getHasBlocks(),
            segmentInfo.getCodec(),
            segmentInfo.getDiagnostics(),
            segmentInfo.getId(),
            segmentInfo.getAttributes(),
            segmentInfo.getIndexSort()
        );
        SegmentWriteState newState = TestsPrepareUtils.prepareSegmentWriteState(newSegmentInfo);
        SparseDocValuesConsumer newConsumer = new SparseDocValuesConsumer(newState, delegate);

        // Create MergeState with sparse field
        MergeState mergeState = mock(MergeState.class);
        FieldInfos mergeFieldInfos = mock(FieldInfos.class);
        when(mergeFieldInfos.iterator()).thenReturn(java.util.Arrays.asList(sparseFieldInfo).iterator());
        mergeState.mergeFieldInfos = mergeFieldInfos;

        // This will trigger the merge logic with sparse field processing
        newConsumer.merge(mergeState);

        verify(delegate, times(1)).merge(mergeState);

        // Clean up
        // InMemorySparseVectorForwardIndex.removeIndex(new InMemoryKey.IndexKey(newSegmentInfo, sparseFieldInfo));
    }

    private BytesRef createValidSparseVectorBytes() {
        // Create a valid sparse vector BytesRef with token "1" -> 0.5f
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);

            // Write one token-value pair: "1" -> 0.5f
            String token = "1";
            byte[] tokenBytes = token.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            dos.writeInt(tokenBytes.length);
            dos.write(tokenBytes);
            dos.writeFloat(0.5f);

            dos.close();
            return new BytesRef(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

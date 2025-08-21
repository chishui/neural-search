/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparsePostingsConsumerTests extends AbstractSparseTestBase {

    private SparsePostingsConsumer sparsePostingsConsumer;
    private FieldsConsumer delegateConsumer;
    private SegmentWriteState segmentWriteState;
    private Directory directory;
    private IndexOutput termsOutput;
    private IndexOutput postingOutput;
    private FieldInfo sparseFieldInfo;
    private FieldInfo nonSparseFieldInfo;
    private Fields fields;
    private NormsProducer normsProducer;
    private MergeState mergeState;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Mock dependencies
        delegateConsumer = mock(FieldsConsumer.class);
        segmentWriteState = mock(SegmentWriteState.class);
        directory = mock(Directory.class);
        termsOutput = mock(IndexOutput.class);
        postingOutput = mock(IndexOutput.class);
        fields = mock(Fields.class);
        normsProducer = mock(NormsProducer.class);
        mergeState = mock(MergeState.class);

        // Setup segment info
        SegmentInfo segmentInfo = mock(SegmentInfo.class);
        when(segmentInfo.name).thenReturn("test_segment");
        when(segmentWriteState.segmentInfo).thenReturn(segmentInfo);
        when(segmentWriteState.directory).thenReturn(directory);
        when(segmentWriteState.segmentSuffix).thenReturn("suffix");
        when(segmentWriteState.context).thenReturn(null);

        // Setup field infos
        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(segmentWriteState.fieldInfos).thenReturn(fieldInfos);

        // Setup sparse field
        sparseFieldInfo = mock(FieldInfo.class);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SparseTokensField.SPARSE_FIELD, "true");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);
        when(sparseFieldInfo.name).thenReturn("sparse_field");
        when(sparseFieldInfo.number).thenReturn(1);
        when(fieldInfos.fieldInfo("sparse_field")).thenReturn(sparseFieldInfo);

        // Setup non-sparse field
        nonSparseFieldInfo = mock(FieldInfo.class);
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());
        when(nonSparseFieldInfo.name).thenReturn("non_sparse_field");
        when(nonSparseFieldInfo.number).thenReturn(2);
        when(fieldInfos.fieldInfo("non_sparse_field")).thenReturn(nonSparseFieldInfo);

        // Setup directory outputs
        when(directory.createOutput(any(), any())).thenReturn(termsOutput, postingOutput);
        when(termsOutput.getFilePointer()).thenReturn(0L);
        when(postingOutput.getFilePointer()).thenReturn(0L);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testConstructor() throws IOException {
        // Test constructor with version parameter
        SparsePostingsConsumer consumer = new SparsePostingsConsumer(delegateConsumer, segmentWriteState, 1);
        assertNotNull(consumer);
    }

    public void testWriteWithNoSparseFields() throws IOException {
        // Create the consumer
        sparsePostingsConsumer = new SparsePostingsConsumer(delegateConsumer, segmentWriteState);

        // Setup fields with only non-sparse fields
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("non_sparse_field");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify delegate was called with the same fields
        verify(delegateConsumer, times(1)).write(any(Fields.class), any(NormsProducer.class));
    }

    public void testWriteWithSparseFields() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup fields with both sparse and non-sparse fields
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("sparse_field");
        fieldNames.add("non_sparse_field");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        // Setup Terms for sparse field
        Terms terms = mock(Terms.class);
        when(fields.terms("sparse_field")).thenReturn(terms);

        // Setup TermsEnum
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(terms.iterator()).thenReturn(termsEnum);
        when(termsEnum.next()).thenReturn(new BytesRef("term1"), null);

        // Setup BlockTermState
        BlockTermState blockTermState = mock(BlockTermState.class);

        // Mock internal methods
        doNothing().when(sparsePostingsConsumer).initWriters();
        doNothing().when(sparsePostingsConsumer).writeFieldCount(anyInt());
        doNothing().when(sparsePostingsConsumer).writeFieldNumber(anyInt());
        doNothing().when(sparsePostingsConsumer).writeTermsSize(anyInt());
        doReturn(blockTermState).when(sparsePostingsConsumer).writeTerm(any(BytesRef.class), any(TermsEnum.class), any(NormsProducer.class));

        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify delegate was called with filtered fields (non-sparse only)
        verify(delegateConsumer, times(1)).write(any(FilterLeafReader.FilterFields.class), any(NormsProducer.class));

        // Verify sparse terms were written
        verify(sparsePostingsConsumer, times(1)).writeFieldCount(1);
        verify(sparsePostingsConsumer, times(1)).writeFieldNumber(1);
        verify(sparsePostingsConsumer, times(1)).writeTermsSize(1);
        verify(sparsePostingsConsumer, times(1)).writeTerm(any(BytesRef.class), any(TermsEnum.class), any(NormsProducer.class));
    }

    public void testMerge() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup merge state
        when(mergeState.segmentInfo).thenReturn(segmentWriteState.segmentInfo);

        // Mock internal methods
        doNothing().when(sparsePostingsConsumer).initWriters();
        doNothing().when(sparsePostingsConsumer).mergeSparseSectors(any(MergeState.class), any(NormsProducer.class));

        sparsePostingsConsumer.merge(mergeState, normsProducer);

        // Verify delegate merge was called
        verify(delegateConsumer, times(1)).merge(mergeState, normsProducer);

        // Verify merge method was called
        verify(sparsePostingsConsumer, times(1)).mergeSparseSectors(mergeState, normsProducer);
    }

    public void testClose() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Mock internal methods
        doNothing().when(sparsePostingsConsumer).closeWriters(anyLong(), anyLong());

        sparsePostingsConsumer.close();

        // Verify delegate close was called
        verify(delegateConsumer, times(1)).close();

        // Verify writers were closed
        verify(sparsePostingsConsumer, times(1)).closeWriters(anyLong(), anyLong());
    }

    public void testMergeWithException() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup merge state
        when(mergeState.segmentInfo).thenReturn(segmentWriteState.segmentInfo);

        // Mock internal methods to throw exception
        doNothing().when(sparsePostingsConsumer).initWriters();
        doThrow(new IOException("Test exception")).when(sparsePostingsConsumer).mergeSparseSectors(any(MergeState.class), any(NormsProducer.class));

        // Should not throw exception, just log error
        sparsePostingsConsumer.merge(mergeState, normsProducer);

        // Verify delegate merge was still called
        verify(delegateConsumer, times(1)).merge(mergeState, normsProducer);
    }

    public void testWriteWithEmptySparseTerms() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup fields with sparse field
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("sparse_field");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        // Return null for terms to simulate empty terms
        when(fields.terms("sparse_field")).thenReturn(null);

        // Mock internal methods
        doNothing().when(sparsePostingsConsumer).initWriters();
        doNothing().when(sparsePostingsConsumer).writeFieldCount(anyInt());
        doNothing().when(sparsePostingsConsumer).writeFieldNumber(anyInt());
        doNothing().when(sparsePostingsConsumer).writeTermsSize(anyInt());

        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify sparse terms size was written as 0
        verify(sparsePostingsConsumer, times(1)).writeTermsSize(0);
    }

    public void testWriteFromMerge() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup fields with sparse field
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("sparse_field");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        // Set fromMerge flag to true by calling merge first
        doNothing().when(sparsePostingsConsumer).initWriters();
        doNothing().when(sparsePostingsConsumer).mergeSparseSectors(any(MergeState.class), any(NormsProducer.class));
        sparsePostingsConsumer.merge(mergeState, normsProducer);

        // Now call write - it should not process sparse fields
        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify sparse terms were not written (since fromMerge is true)
        verify(sparsePostingsConsumer, times(0)).writeFieldCount(anyInt());
    }

    public void testIteratorFiltering() throws IOException {
        // Create the consumer
        sparsePostingsConsumer = new SparsePostingsConsumer(delegateConsumer, segmentWriteState);

        // Setup fields with both sparse and non-sparse fields
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("sparse_field");
        fieldNames.add("non_sparse_field");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        // Capture the filtered iterator to verify filtering
        final List<String> capturedFields = new ArrayList<>();
        when(delegateConsumer.write(any(Fields.class), any(NormsProducer.class))).thenAnswer(invocation -> {
            Fields filteredFields = invocation.getArgument(0);
            Iterator<String> iterator = filteredFields.iterator();
            while (iterator.hasNext()) {
                capturedFields.add(iterator.next());
            }
            return null;
        });

        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify only non-sparse fields were passed to delegate
        assertEquals(1, capturedFields.size());
        assertEquals("non_sparse_field", capturedFields.get(0));
    }

    public void testWriteWithMultipleSparseFields() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup two sparse fields
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("sparse_field");
        fieldNames.add("sparse_field2");
        when(fields.iterator()).thenReturn(fieldNames.iterator());

        // Setup second sparse field info
        FieldInfo sparseFieldInfo2 = mock(FieldInfo.class);
        Map<String, String> sparseAttributes2 = new HashMap<>();
        sparseAttributes2.put(SparseTokensField.SPARSE_FIELD, "true");
        when(sparseFieldInfo2.attributes()).thenReturn(sparseAttributes2);
        when(sparseFieldInfo2.name).thenReturn("sparse_field2");
        when(sparseFieldInfo2.number).thenReturn(3);
        when(segmentWriteState.fieldInfos.fieldInfo("sparse_field2")).thenReturn(sparseFieldInfo2);

        // Setup Terms for both sparse fields
        Terms terms1 = mock(Terms.class);
        Terms terms2 = mock(Terms.class);
        when(fields.terms("sparse_field")).thenReturn(terms1);
        when(fields.terms("sparse_field2")).thenReturn(terms2);

        // Setup TermsEnum for both fields
        TermsEnum termsEnum1 = mock(TermsEnum.class);
        TermsEnum termsEnum2 = mock(TermsEnum.class);
        when(terms1.iterator()).thenReturn(termsEnum1);
        when(terms2.iterator()).thenReturn(termsEnum2);
        when(termsEnum1.next()).thenReturn(new BytesRef("term1"), null);
        when(termsEnum2.next()).thenReturn(new BytesRef("term2"), null);

        // Setup BlockTermState
        BlockTermState blockTermState = mock(BlockTermState.class);

        // Mock internal methods
        doNothing().when(sparsePostingsConsumer).initWriters();
        doNothing().when(sparsePostingsConsumer).writeFieldCount(anyInt());
        doNothing().when(sparsePostingsConsumer).writeFieldNumber(anyInt());
        doNothing().when(sparsePostingsConsumer).writeTermsSize(anyInt());
        doReturn(blockTermState).when(sparsePostingsConsumer).writeTerm(any(BytesRef.class), any(TermsEnum.class), any(NormsProducer.class));

        sparsePostingsConsumer.write(fields, normsProducer);

        // Verify field count is 2
        verify(sparsePostingsConsumer, times(1)).writeFieldCount(2);

        // Verify both field numbers were written
        verify(sparsePostingsConsumer, times(1)).writeFieldNumber(1);
        verify(sparsePostingsConsumer, times(1)).writeFieldNumber(3);

        // Verify terms size was written for both fields
        verify(sparsePostingsConsumer, times(2)).writeTermsSize(1);

        // Verify terms were written for both fields
        verify(sparsePostingsConsumer, times(2)).writeTerm(any(BytesRef.class), any(TermsEnum.class), any(NormsProducer.class));
    }

    public void testMergeSparseSectors() throws IOException {
        // Create a spy of SparsePostingsConsumer to mock internal methods
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Setup merge state with sparse fields
        MergeStateFacade mergeStateFacade = mock(MergeStateFacade.class);
        when(mergeStateFacade.getNumSparseFields()).thenReturn(1);
        when(mergeStateFacade.getSparseFieldNumber(0)).thenReturn(1);
        when(mergeStateFacade.getSparseFieldName(0)).thenReturn("sparse_field");
        when(mergeStateFacade.getNumTerms(0)).thenReturn(1);
        when(mergeStateFacade.getTerm(0, 0)).thenReturn(new BytesRef("term1"));

        // Mock internal methods
        doReturn(mergeStateFacade).when(sparsePostingsConsumer).createMergeStateFacade(any(MergeState.class));
        doNothing().when(sparsePostingsConsumer).writeFieldCount(anyInt());
        doNothing().when(sparsePostingsConsumer).writeFieldNumber(anyInt());
        doNothing().when(sparsePostingsConsumer).writeTermsSize(anyInt());
        doNothing().when(sparsePostingsConsumer).writeMergedTerm(any(BytesRef.class), any(MergeStateFacade.class), anyInt(), anyInt(), any(NormsProducer.class));

        // Call the method directly
        sparsePostingsConsumer.mergeSparseSectors(mergeState, normsProducer);

        // Verify merge operations
        verify(sparsePostingsConsumer, times(1)).writeFieldCount(1);
        verify(sparsePostingsConsumer, times(1)).writeFieldNumber(1);
        verify(sparsePostingsConsumer, times(1)).writeTermsSize(1);
        verify(sparsePostingsConsumer, times(1)).writeMergedTerm(any(BytesRef.class), any(MergeStateFacade.class), anyInt(), anyInt(), any(NormsProducer.class));
    }

    public void testInitWriters() throws IOException {
        // Create a spy of SparsePostingsConsumer
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Call the method directly
        sparsePostingsConsumer.initWriters();

        // Verify directory.createOutput was called twice
        verify(directory, times(2)).createOutput(any(), any());
    }

    public void testCloseWriters() throws IOException {
        // Create a spy of SparsePostingsConsumer
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(delegateConsumer, segmentWriteState));

        // Initialize writers first
        sparsePostingsConsumer.initWriters();

        // Call the method directly
        sparsePostingsConsumer.closeWriters(0L, 0L);

        // Verify outputs were closed
        verify(termsOutput, times(1)).close();
        verify(postingOutput, times(1)).close();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
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
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparsePostingsConsumerTests extends AbstractSparseTestBase {

    @Mock
    private FieldsConsumer mockDelegate;

    @Mock
    private Directory mockDirectory;

    @Mock
    private IndexOutput mockTermsOutput;

    @Mock
    private IndexOutput mockPostingOutput;

    @Mock
    private SparseTermsLuceneWriter mockSparseTermsLuceneWriter;

    @Mock
    private ClusteredPostingTermsWriter mockClusteredPostingTermsWriter;

    @Mock
    private Fields mockFields;

    @Mock
    private NormsProducer mockNormsProducer;

    @Mock
    private MergeState mockMergeState;

    @Mock
    private SparsePostingsReader mockSparsePostingsReader;

    private SegmentWriteState mockWriteState;
    private FieldInfos mockFieldInfos;
    private SparsePostingsConsumer sparsePostingsConsumer;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Setup directory and outputs
        when(mockDirectory.createOutput(anyString(), any())).thenReturn(mockTermsOutput, mockPostingOutput);

        // Setup segment write state
        SegmentInfo mockSegmentInfo = spy(TestsPrepareUtils.prepareSegmentInfo());
        mockFieldInfos = mock(FieldInfos.class);
        mockWriteState = new SegmentWriteState(null, mockDirectory, mockSegmentInfo, mockFieldInfos, null, null, null);

        // Create the consumer with mocked dependencies
        sparsePostingsConsumer = spy(new SparsePostingsConsumer(mockDelegate, mockWriteState));
    }

    /**
     * Test constructor with default version
     */
    public void test_constructor_withDefaultVersion() throws IOException {
        SparsePostingsConsumer consumer = new SparsePostingsConsumer(mockDelegate, mockWriteState);
        assertNotNull("Consumer should be created", consumer);
    }

    /**
     * Test constructor with specific version
     */
    public void test_constructor_withSpecificVersion() throws IOException {
        SparsePostingsConsumer consumer = new SparsePostingsConsumer(mockDelegate, mockWriteState, 1);
        assertNotNull("Consumer should be created", consumer);
    }

    /**
     * Test constructor handles IOException properly
     */
    public void test_constructor_withIOException() throws IOException {
        when(mockDirectory.createOutput(anyString(), any())).thenThrow(new IOException("Test exception"));

        IOException exception = expectThrows(IOException.class, () -> {
            new SparsePostingsConsumer(mockDelegate, mockWriteState);
        });

        assertEquals("Test exception", exception.getMessage());
    }

    /**
     * Test write method with no sparse fields
     */
    public void test_write_withNonSparseFields() throws IOException {
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("regular_field1");
        fieldNames.add("regular_field2");

        when(mockFields.iterator()).thenReturn(fieldNames.iterator());

        // Setup field info to indicate non-sparse fields
        FieldInfo mockFieldInfo = mock(FieldInfo.class);
        when(mockFieldInfos.fieldInfo(anyString())).thenReturn(mockFieldInfo);

        // Call write
        sparsePostingsConsumer.write(mockFields, mockNormsProducer);

        // Verify delegate was called with all fields
        verify(mockDelegate).write(any(Fields.class), eq(mockNormsProducer));

        // Verify sparse writers were not used
        verify(mockSparseTermsLuceneWriter, never()).writeFieldCount(any(Integer.class));
    }

    /**
     * Test write method with sparse fields
     */
    @SneakyThrows
    public void test_write_withSparseFields() {
        // Setup fields with sparse fields
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("regular_field");
        fieldNames.add("sparse_field");

        when(mockFields.iterator()).thenReturn(fieldNames.iterator());

        // Setup field info to indicate sparse field
        FieldInfo regularFieldInfo = mock(FieldInfo.class);
        FieldInfo sparseFieldInfo = mock(FieldInfo.class);

        when(mockFieldInfos.fieldInfo("regular_field")).thenReturn(regularFieldInfo);
        when(mockFieldInfos.fieldInfo("sparse_field")).thenReturn(sparseFieldInfo);

        // Setup terms for sparse field
        Terms mockTerms = mock(Terms.class);
        TermsEnum mockTermsEnum = mock(TermsEnum.class);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);
        when(mockFields.terms("sparse_field")).thenReturn(mockTerms);

        // Setup terms enum to return one term then null
        BytesRef term = new BytesRef("term1");
        when(mockTermsEnum.next()).thenReturn(term, null);

        // Setup clustered posting writer to return a term state
        BlockTermState mockTermState = mock(BlockTermState.class);
        when(mockClusteredPostingTermsWriter.write(any(BytesRef.class), eq(mockTermsEnum), eq(mockNormsProducer))).thenReturn(
            mockTermState
        );

        // Call write
        sparsePostingsConsumer.write(mockFields, mockNormsProducer);

        // Verify delegate was called with non-sparse fields
        verify(mockDelegate).write(any(FilterLeafReader.FilterFields.class), eq(mockNormsProducer));

        // Verify sparse writers were used
        verify(mockSparseTermsLuceneWriter).writeFieldCount(1);
        verify(mockSparseTermsLuceneWriter).writeFieldNumber(any(Integer.class));
        verify(mockSparseTermsLuceneWriter).writeTermsSize(1);
        verify(mockSparseTermsLuceneWriter).writeTerm(eq(term), eq(mockTermState));
    }

    /**
     * Test merge method
     */
    @SneakyThrows
    public void test_merge() {
        sparsePostingsConsumer.merge(mockMergeState, mockNormsProducer);

        // Verify delegate merge was called
        verify(mockDelegate).merge(mockMergeState, mockNormsProducer);

        // Verify SparsePostingsReader merge was called
        verify(mockSparsePostingsReader).merge(mockSparseTermsLuceneWriter, mockClusteredPostingTermsWriter);
    }

    /**
     * Test merge method handles exceptions
     */
    @SneakyThrows
    public void test_merge_withExceptions() {
        // Mock SparsePostingsReader to throw exception
        doThrow(new RuntimeException("Test exception")).when(mockSparsePostingsReader).merge(any(), any());

        // Call merge - should not throw exception
        sparsePostingsConsumer.merge(mockMergeState, mockNormsProducer);

        // Verify delegate merge was still called
        verify(mockDelegate).merge(mockMergeState, mockNormsProducer);
    }

    /**
     * Test close method with successful close
     */
    @SneakyThrows
    public void test_close() {
        // Setup mocks for close
        doNothing().when(mockSparseTermsLuceneWriter).close(anyLong());
        doNothing().when(mockClusteredPostingTermsWriter).close(anyLong());

        // Call close
        sparsePostingsConsumer.close();

        // Verify delegate close was called
        verify(mockDelegate).close();

        // Verify writers were closed
        verify(mockSparseTermsLuceneWriter).close(anyLong());
        verify(mockClusteredPostingTermsWriter).close(anyLong());

        // Verify outputs were closed
        verify(mockTermsOutput).close();
        verify(mockPostingOutput).close();
    }

    /**
     * Test close method handles exceptions from writers
     */
    @SneakyThrows
    public void test_close_withExceptions() {
        // Setup mocks for close with exception
        doThrow(new IOException("Test exception")).when(mockSparseTermsLuceneWriter).close(anyLong());

        // Call close - should propagate exception
        IOException exception = expectThrows(IOException.class, () -> { sparsePostingsConsumer.close(); });

        assertEquals("Test exception", exception.getMessage());

        // Verify delegate close was still called
        verify(mockDelegate).close();

        // Verify outputs were closed with exception handling
        verify(mockTermsOutput).close();
        verify(mockPostingOutput).close();
    }
}

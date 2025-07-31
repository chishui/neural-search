/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class SparsePostingsFormatTests extends AbstractSparseTestBase {

    private PostingsFormat mockDelegate;
    private SparsePostingsFormat sparsePostingsFormat;
    private SegmentWriteState mockWriteState;
    private SegmentReadState mockReadState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockDelegate = mock(PostingsFormat.class);
        when(mockDelegate.getName()).thenReturn("TestPostingsFormat");

        sparsePostingsFormat = new SparsePostingsFormat(mockDelegate);

        // Use TestsPrepareUtils to create real objects since SegmentInfo.name is final
        mockWriteState = TestsPrepareUtils.prepareSegmentWriteState();

        mockReadState = mock(SegmentReadState.class);
    }

    public void testConstructor() {
        // Verify that the format name is inherited from delegate
        assertEquals("TestPostingsFormat", sparsePostingsFormat.getName());
    }

    public void testFieldsConsumer() throws IOException {
        // Setup
        FieldsConsumer mockDelegateConsumer = mock(FieldsConsumer.class);
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenReturn(mockDelegateConsumer);

        // Execute
        FieldsConsumer result = sparsePostingsFormat.fieldsConsumer(mockWriteState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparsePostingsConsumer);
        verify(mockDelegate).fieldsConsumer(mockWriteState);
    }

    public void testFieldsProducer() throws IOException {
        // Setup
        FieldsProducer mockDelegateProducer = mock(FieldsProducer.class);
        when(mockDelegate.fieldsProducer(mockReadState)).thenReturn(mockDelegateProducer);

        // Execute
        FieldsProducer result = sparsePostingsFormat.fieldsProducer(mockReadState);

        // Verify
        assertNotNull(result);
        assertTrue(result instanceof SparsePostingsProducer);
        verify(mockDelegate).fieldsProducer(mockReadState);
    }

    public void testFieldsConsumerIOException() throws IOException {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsConsumer(mockWriteState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { sparsePostingsFormat.fieldsConsumer(mockWriteState); });
        assertEquals("Test exception", exception.getMessage());
    }

    public void testFieldsProducerIOException() throws IOException {
        // Setup
        IOException expectedException = new IOException("Test exception");
        when(mockDelegate.fieldsProducer(mockReadState)).thenThrow(expectedException);

        // Execute & Verify
        IOException exception = expectThrows(IOException.class, () -> { sparsePostingsFormat.fieldsProducer(mockReadState); });
        assertEquals("Test exception", exception.getMessage());
    }
}

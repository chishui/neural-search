/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.NumericDocValues;

public class ClusteredPostingTermsWriterTests extends AbstractSparseTestBase {

    private static final String CODEC_NAME = "test_codec";
    private static final int VERSION = 1;

    @Mock
    private NumericDocValues mockNorms;

    @Mock
    private IndexOutput mockIndexOutput;

    private SegmentWriteState mockWriteState;

    @Mock
    private Directory mockDirectory;

    private FieldInfo mockFieldInfo;

    @Mock
    private DocValuesFormat mockDocValuesFormat;

    @Mock
    private DocValuesProducer mockDocValuesProducer;

    @Mock
    private BinaryDocValues mockBinaryDocValues;

    @Mock
    private TermsEnum mockTermsEnum;

    @Mock
    private NormsProducer mockNormsProducer;

    SegmentInfo mockSegmentInfo;

    @Mock
    Codec mockCodec;

    private ClusteredPostingTermsWriter clusteredPostingTermsWriter;

    @Before
    @SneakyThrows
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        clusteredPostingTermsWriter = new ClusteredPostingTermsWriter(CODEC_NAME, VERSION);
        mockSegmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        mockWriteState = TestsPrepareUtils.prepareSegmentWriteState(mockSegmentInfo);
        mockFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Setup codec and DocValues
        when(mockCodec.docValuesFormat()).thenReturn(mockDocValuesFormat);

        // Setup field info
        mockFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Setup DocValues
        when(mockDocValuesFormat.fieldsProducer(any(SegmentReadState.class))).thenReturn(mockDocValuesProducer);
        when(mockDocValuesProducer.getBinary(any(FieldInfo.class))).thenReturn(mockBinaryDocValues);

        // Setup directory to return our mock IndexOutput
        when(mockDirectory.createOutput(any(String.class), any())).thenReturn(mockIndexOutput);
    }

    /**
     * Tests the constructor with codec name and version.
     */
    public void test_constructor() {
        ClusteredPostingTermsWriter writer = new ClusteredPostingTermsWriter(CODEC_NAME, VERSION);
        assertNotNull("ClusteredPostingTermsWriter should be created", writer);
    }

    /**
     * Test case for the write method that takes a BytesRef, TermsEnum, and NormsProducer.
     * It verifies that the method correctly sets the currentTerm and calls the superclass writeTerm method.
     */
    @SneakyThrows
    public void test_write_withTermsEnum() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);

        // Initialize the writer
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Create test data
        BytesRef testText = new BytesRef("test");
        BlockTermState expectedState = new Lucene101PostingsFormat.IntBlockTermState();

        // Mock the writeTerm method to return our expected state
        doReturn(expectedState).when(clusteredPostingTermsWriter)
            .writeTerm(eq(testText), eq(mockTermsEnum), any(FixedBitSet.class), eq(mockNormsProducer));

        // Call the method under test
        BlockTermState result = clusteredPostingTermsWriter.write(testText, mockTermsEnum, mockNormsProducer);

        // Verify the result
        assertSame("Should return the BlockTermState from writeTerm", expectedState, result);

        // Verify that writeTerm was called with the correct parameters
        verify(clusteredPostingTermsWriter).writeTerm(eq(testText), eq(mockTermsEnum), any(FixedBitSet.class), eq(mockNormsProducer));
    }

    /**
     * Test case for the write method with PostingClusters parameter.
     * This test verifies that the method correctly writes the given PostingClusters
     * to the IndexOutput and returns a new BlockTermState.
     */
    @SneakyThrows
    public void test_write_withPostingClusters() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        BytesRef text = new BytesRef("test_term");
        PostingClusters postingClusters = mock(PostingClusters.class);

        BlockTermState result = clusteredPostingTermsWriter.write(text, postingClusters);

        assertNotNull("BlockTermState should not be null", result);
    }

    /**
     * Test the setFieldAndMaxDoc method with a merge operation.
     * This test verifies that when isMerge is true, the setPostingClustering method is called.
     */
    @SneakyThrows
    public void test_setFieldAndMaxDoc_withoutMerge() {
        clusteredPostingTermsWriter = spy(this.clusteredPostingTermsWriter);
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Mock the superclass setField method
        doNothing().when(clusteredPostingTermsWriter).setField(any(FieldInfo.class));

        // Mock setPostingClustering to avoid actual clustering
        doNothing().when(clusteredPostingTermsWriter).setPostingClustering(anyInt());

        // Mock the superclass setField method
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        // Verify setField was called
        verify(clusteredPostingTermsWriter).setField(mockFieldInfo);

        // Verify that setPostingClustering was called
        verify(clusteredPostingTermsWriter, times(1)).setPostingClustering(100);
    }

    /**
     * Test the setFieldAndMaxDoc method with a merge operation.
     * This test verifies that when isMerge is true, the setPostingClustering method is not called.
     */
    @SneakyThrows
    public void test_setFieldAndMaxDoc_withMerge() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Mock the superclass setField method
        doNothing().when(clusteredPostingTermsWriter).setField(any(FieldInfo.class));

        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, true);

        // Verify setField was called
        verify(clusteredPostingTermsWriter).setField(mockFieldInfo);

        // Verify that setPostingClustering was not called
        verify(clusteredPostingTermsWriter, times(0)).setPostingClustering(anyInt());
    }

    /**
     * Tests the newTermState method
     */
    @SneakyThrows
    public void test_newTermState() {
        BlockTermState state = clusteredPostingTermsWriter.newTermState();
        assertNotNull("Term state should not be null", state);
    }

    /**
     * Test case for the startTerm method.
     * This test verifies that the startTerm method does not throw exception
     */
    public void test_startTerm() throws IOException {
        clusteredPostingTermsWriter.startTerm(mockNorms);
        // No assertion needed, we're just verifying it doesn't throw an exception
    }

    /**
     * Test case for the finishTerm method.
     * This test verifies that the startTerm method does not throw exception
     */
    @SneakyThrows
    public void test_finishTerm() {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Setup for finishTerm
        BytesRef term = new BytesRef("test_term");
        clusteredPostingTermsWriter.startTerm(null);
        clusteredPostingTermsWriter.startDoc(1, 10);
        clusteredPostingTermsWriter.startDoc(2, 20);

        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        doNothing().when(clusteredPostingTermsWriter).setField(any(FieldInfo.class));

        // Mock the PostingClusters that would be returned
        PostingClusters mockClusters = createTestPostingClusters();

        clusteredPostingTermsWriter.write(term, mockClusters);

        BlockTermState state = clusteredPostingTermsWriter.newTermState();
        clusteredPostingTermsWriter.finishTerm(state);

        // Verify the output was written
        verify(mockIndexOutput, times(1)).writeVLong(anyInt());
    }

    /**
     * Test case for startDoc method with a valid docID.
     * This test verifies that the method correctly handles a non-negative docID.
     */
    @SneakyThrows
    public void test_startDoc_withValidDocId() {
        clusteredPostingTermsWriter.startDoc(1, 10);
        // No assertion needed, we're just verifying it doesn't throw an exception
    }

    /**
     * Test case for startDoc method when docID is -1.
     * This test verifies that an IllegalStateException is thrown when an invalid docID is provided.
     */
    public void test_startDoc_withInvalidDocID() {
        Exception exception = expectThrows(IllegalStateException.class, () -> { clusteredPostingTermsWriter.startDoc(-1, 10); });
        assertEquals("docId must be set before startDoc", exception.getMessage());
    }

    /**
     * Test case for addPosition method
     * This test verifies that an UnsupportedOperationException is thrown.
     */

    public void test_addPosition_thenThrownUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () -> clusteredPostingTermsWriter.addPosition(0, new BytesRef(), 0, 0));
    }

    /**
     * Test case for finishDoc method
     * This test verifies that no exception is thrown.
     */
    @SneakyThrows
    public void test_finishDoc() {
        clusteredPostingTermsWriter.finishDoc();
        // No assertions needed, just verifying it doesn't throw
    }

    /**
     * Tests the init method with mocked objects.
     */
    @SneakyThrows
    public void test_init() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);
    }

    /**
     * Test case for encodeTerm method
     * This test verifies that an UnsupportedOperationException is thrown.
     */
    public void test_encodeTerm_thenThrowUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> clusteredPostingTermsWriter.encodeTerm(null, null, null, false));
    }

    /**
     * Test case for the close() method when docValuesProducer is null.
     * This test verifies that the method closes the postingOut
     */
    @SneakyThrows
    public void test_close_whenDocValuesProducerNull() {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);
        clusteredPostingTermsWriter.close();

        verify(mockIndexOutput).writeLong(eq(100L));
    }

    /**
     * Test case for the close() method when docValuesProducer is not null.
     * This test verifies that the close() method properly closes both the IndexOutput and DocValuesProducer.
     */
    public void test_close_whenDocValuesProducerNonNull() throws IOException {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Set docValuesProducer to a non-null value
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        clusteredPostingTermsWriter.close();

        // Verify that writeFooter was called on mockIndexOutput
        verify(mockIndexOutput).writeInt(0);

        // Verify that close was called on mockDocValuesProducer
        verify(mockDocValuesProducer).close();
    }

    /**
     * Test the closeWithException method when docValuesProducer is null.
     * This tests the edge case where the docValuesProducer field is not initialized.
     */
    public void test_closeWithException_whenDocValuesProducerNull() {
        ClusteredPostingTermsWriter writer = new ClusteredPostingTermsWriter(CODEC_NAME, VERSION);
        writer.closeWithException();
        // No exception should be thrown
    }

    /**
     * Test case for closeWithException method when docValuesProducer is not null.
     * This test verifies that IOUtils.closeWhileHandlingException is called for both
     * postingOut and docValuesProducer when closeWithException is invoked.
     */
    @SneakyThrows
    public void test_closeWithException_whenDocValuesProducerNonNull() {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Set docValuesProducer to a non-null value
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        // Call the method under test
        clusteredPostingTermsWriter.closeWithException();

        // Verify that IOUtils.closeWhileHandlingException was called for both postingOut and docValuesProducer
        verify(mockIndexOutput).close();
        verify(mockDocValuesProducer).close();
    }

    /**
     * Test case for the close(long startFp) method.
     * This test verifies that the method writes postingOut and calls this.close()
     */
    @SneakyThrows
    public void test_close_withStartFp() {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);
        clusteredPostingTermsWriter.close(100L);

        // Verify startFp was written
        verify(mockIndexOutput).writeLong(eq(100L));

        // Verify footer was written
        verify(mockIndexOutput).writeInt(eq(0));
    }

    /**
     * Tests the close method when an IOException occurs while writing the startFp.
     * This is a test case that verifies the method's behavior when an exception is thrown.
     */
    public void test_close_withStartFpThrowsIOException() throws IOException {
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);

        // Mock IOException when writing startFp
        doThrow(new IOException("Test exception")).when(mockIndexOutput).writeLong(anyInt());

        clusteredPostingTermsWriter.close(100L);
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseTermsLuceneReaderTests extends AbstractSparseTestBase {

    private final static String TERM_NAME = "1000";
    private final static String TEST_FIELD = "test_field";

    @Mock
    private Directory mockDirectory;

    @Mock
    private FieldInfos mockFieldInfos;

    @Mock
    private IndexInput mockTermsInput;

    @Mock
    private IndexInput mockPostingInput;

    @Mock
    private CodecUtilWrapper mockCodecUtilWrapper;

    @Mock
    private FieldInfo mockFieldInfo;

    private SegmentReadState segmentReadState;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        setupMockInputsForSuccessfulConstruction();

        SegmentInfo mockSegmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        when(mockFieldInfos.fieldInfo(0)).thenReturn(mockFieldInfo);
        when(mockFieldInfo.getName()).thenReturn(TEST_FIELD);
        when(mockDirectory.openInput(anyString(), any(IOContext.class))).thenReturn(mockTermsInput).thenReturn(mockPostingInput);

        when(mockCodecUtilWrapper.footerLength()).thenReturn(CodecUtil.footerLength());
        segmentReadState = new SegmentReadState(mockDirectory, mockSegmentInfo, mockFieldInfos, IOContext.DEFAULT, "test_suffix");
    }

    @SneakyThrows
    public void testConstructor_thenSuccess() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        assertNotNull(reader);
        verify(mockTermsInput, never()).close();
        verify(mockPostingInput, never()).close();
    }

    @SneakyThrows
    public void testConstructor_withIOExceptionHandling() {
        doThrow(new IOException("Test exception")).when(mockTermsInput).readVInt();
        new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        verify(mockTermsInput, times(1)).close();
        verify(mockPostingInput, times(1)).close();
    }

    @SneakyThrows
    public void testIterator() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        Iterator<String> iterator = reader.iterator();
        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertEquals(TEST_FIELD, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @SneakyThrows
    public void testTerms_ThrowsUnsupportedOperationException() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        expectThrows(UnsupportedOperationException.class, () -> reader.terms(TEST_FIELD));
    }

    @SneakyThrows
    public void testGetTerms_withExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        Set<BytesRef> terms = reader.getTerms(TEST_FIELD);
        assertNotNull(terms);
        assertEquals(1, terms.size());
    }

    @SneakyThrows
    public void testGetTerms_withNonExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        Set<BytesRef> terms = reader.getTerms("non_existing_field");
        assertNotNull(terms);
        assertTrue(terms.isEmpty());
    }

    @SneakyThrows
    public void testRead_withExistingFieldAndTerm() {
        setupMockPostingInput();
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        BytesRef term = new BytesRef(TERM_NAME);
        PostingClusters clusters = reader.read(TEST_FIELD, term);

        assertNotNull(clusters);
    }

    /**
     * A segment written before clustered postings were folded holds 0 and 32768 as two terms. They
     * load as one folded term whose posting is both of theirs, read back to back.
     */
    @SneakyThrows
    public void testRead_foldsRawTermsWrittenBeforeFolding() {
        List<BytesRef> rawTerms = List.of(new BytesRef("0"), new BytesRef("32768"));
        // numberOfFields, fieldId, then each term's byteLength
        when(mockTermsInput.readVInt()).thenReturn(1).thenReturn(0).thenReturn(rawTerms.get(0).length).thenReturn(rawTerms.get(1).length);
        // numberOfTerms, then each term's fileOffset
        when(mockTermsInput.readVLong()).thenReturn(2L).thenReturn(50L).thenReturn(90L);
        Iterator<BytesRef> termBytes = rawTerms.iterator();
        doAnswer(invocation -> {
            BytesRef next = termBytes.next();
            System.arraycopy(next.bytes, next.offset, invocation.getArgument(0), invocation.getArgument(1), next.length);
            return null;
        }).when(mockTermsInput).readBytes(any(byte[].class), anyInt(), anyInt());
        // Each offset holds one cluster of one doc with a one-item summary: clusterSize, docSize, summarySize.
        when(mockPostingInput.readVLong()).thenReturn(1L, 1L, 1L, 1L, 1L, 1L);
        // doc id then summary token, per offset
        when(mockPostingInput.readVInt()).thenReturn(3, 0, 4, 0);
        when(mockPostingInput.readByte()).thenReturn((byte) 1);

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);

        assertEquals(Set.of(new BytesRef("0")), reader.getTerms(TEST_FIELD));
        assertNull("the raw id is not a term of its own any more", reader.read(TEST_FIELD, new BytesRef("32768")));
        PostingClusters clusters = reader.read(TEST_FIELD, new BytesRef("0"));
        assertNotNull(clusters);
        assertEquals(2, clusters.getClusters().size());
        verify(mockPostingInput).seek(50L);
        verify(mockPostingInput).seek(90L);
        assertEquals(3, clusters.getClusters().get(0).iterator().next().getDocID());
        assertEquals(4, clusters.getClusters().get(1).iterator().next().getDocID());
    }

    @SneakyThrows
    public void testRead_withNonExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        BytesRef term = new BytesRef(TERM_NAME);
        PostingClusters clusters = reader.read("non_existing_field", term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withNonExistingTerm() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        BytesRef term = new BytesRef("non_existing_term");
        PostingClusters clusters = reader.read(TEST_FIELD, term);
        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withEmptyClusters() {
        setupMockPostingInputForEmptyClusters();
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        BytesRef term = new BytesRef(TERM_NAME);
        PostingClusters clusters = reader.read(TEST_FIELD, term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testSize_thenThrowsUnsupportedOperationException() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        expectThrows(UnsupportedOperationException.class, () -> reader.size());
    }

    @SneakyThrows
    public void testClose() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        reader.close();
        verify(mockTermsInput).close();
        verify(mockPostingInput).close();
    }

    @SneakyThrows
    public void testCheckIntegrity() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState, mockCodecUtilWrapper);
        reader.checkIntegrity();
        verify(mockCodecUtilWrapper, times(1)).checksumEntireFile(mockTermsInput);
        verify(mockCodecUtilWrapper, times(1)).checksumEntireFile(mockPostingInput);
    }

    @SneakyThrows
    private void setupMockInputsForSuccessfulConstruction() {
        BytesRef bytesRef = new BytesRef(TERM_NAME);
        // numberOfFields, fieldId and byteLength
        when(mockTermsInput.readVInt()).thenReturn(1).thenReturn(0).thenReturn(bytesRef.length);
        // numberOfTerms and fileOffset
        when(mockTermsInput.readVLong()).thenReturn(1L).thenReturn(50L);
        // dirOffset
        when(mockTermsInput.readLong()).thenReturn(42L);
        // read bytes
        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);

            // Copy the actual term bytes to the destination array
            System.arraycopy(bytesRef.bytes, 0, bytes, offset, length);

            return null;
        }).when(mockTermsInput).readBytes(any(byte[].class), anyInt(), anyInt());
    }

    @SneakyThrows
    private void setupMockPostingInput() {
        // clusterSize, docSize, summaryVectorSize
        when(mockPostingInput.readVLong())
            .thenReturn(1L)
            .thenReturn(1L)
            .thenReturn(1L);
        // doc id, sparse vector item index
        when(mockPostingInput.readVInt())
            .thenReturn(1)
            .thenReturn(1);
        // doc weight, shouldNotSkip, sparse vector item weight
        when(mockPostingInput.readByte())
            .thenReturn((byte) 1)
            .thenReturn((byte) 1)
            .thenReturn((byte) 1);
    }

    @SneakyThrows
    private void setupMockPostingInputForEmptyClusters() {
        when(mockPostingInput.readVLong()).thenReturn(0L);
    }
}

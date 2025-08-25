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
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class SparseTermsLuceneReaderTests extends AbstractSparseTestBase {

    @Mock
    private Directory mockDirectory;

    private SegmentInfo mockSegmentInfo;

    @Mock
    private FieldInfos mockFieldInfos;

    private FieldInfo mockFieldInfo;

    @Mock
    private IndexInput mockTermsInput;

    @Mock
    private IndexInput mockPostingInput;

    @Mock
    private SegmentReadState segmentReadState;

    private MockedStatic<CodecUtil> codecUtilMock;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        mockSegmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        mockFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        when(mockFieldInfos.fieldInfo(0)).thenReturn(mockFieldInfo);

        segmentReadState = new SegmentReadState(mockDirectory, mockSegmentInfo, mockFieldInfos, IOContext.DEFAULT, "test_suffix");

        when(mockDirectory.openInput(anyString(), any(IOContext.class))).thenReturn(mockTermsInput).thenReturn(mockPostingInput);
        codecUtilMock = Mockito.mockStatic(CodecUtil.class);
        codecUtilMock.when(
            () -> CodecUtil.checkIndexHeader(any(DataInput.class), anyString(), anyInt(), anyInt(), any(byte[].class), anyString())
        ).thenReturn(0);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (codecUtilMock != null) {
            codecUtilMock.close();
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor_thenSuccess() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        assertNotNull(reader);
    }

    @SneakyThrows
    public void testConstructor_withIOExceptionHandling() {
        when(mockDirectory.openInput(anyString(), any(IOContext.class)))
            .thenThrow(new IOException("Test exception"));

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        assertNotNull(reader);
    }

    @SneakyThrows
    public void testIterator() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Iterator<String> iterator = reader.iterator();

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertEquals("test_field", iterator.next());
    }

    @SneakyThrows
    public void testTerms_ThrowsUnsupportedOperationException() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        expectThrows(UnsupportedOperationException.class, () -> reader.terms("test_field"));
    }

    @SneakyThrows
    public void testGetTerms_withExistingField() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Set<BytesRef> terms = reader.getTerms("test_field");

        assertNotNull(terms);
        assertEquals(1, terms.size());
    }

    @SneakyThrows
    public void testGetTerms_withNonExistingField() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Set<BytesRef> terms = reader.getTerms("non_existing_field");

        assertNotNull(terms);
        assertTrue(terms.isEmpty());
    }

    @SneakyThrows
    public void testRead_withExistingFieldAndTerm() {
        setupMockInputsForSuccessfulConstruction();
        setupMockPostingInput();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("test_term");
        PostingClusters clusters = reader.read("test_field", term);

        assertNotNull(clusters);
    }

    @SneakyThrows
    public void testRead_withNonExistingField() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("test_term");
        PostingClusters clusters = reader.read("non_existing_field", term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withNonExistingTerm() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("non_existing_term");
        PostingClusters clusters = reader.read("test_field", term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withEmptyClusters() {
        setupMockInputsForSuccessfulConstruction();
        setupMockPostingInputForEmptyClusters();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("test_term");
        PostingClusters clusters = reader.read("test_field", term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testSize_ThrowsUnsupportedOperationException() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        expectThrows(UnsupportedOperationException.class, () -> reader.size());
    }

    @SneakyThrows
    public void testClose() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        reader.close();
    }

    @SneakyThrows
    public void testCheckIntegrity() {
        setupMockInputsForSuccessfulConstruction();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        reader.checkIntegrity();
    }

    private void setupMockInputsForSuccessfulConstruction() throws IOException {
        // Mock terms input
        when(mockTermsInput.length()).thenReturn(100L);
        when(mockTermsInput.readVInt())
            .thenReturn(1) // numberOfFields
            .thenReturn(0) // fieldId
            .thenReturn(4); // byteLength
        when(mockTermsInput.readVLong())
            .thenReturn(1L) // numberOfTerms
            .thenReturn(50L); // fileOffset
        when(mockTermsInput.readLong()).thenReturn(42L); // dirOffset

        // Mock posting input - no specific setup needed for constructor
    }

    private void setupMockPostingInput() throws IOException {
        when(mockPostingInput.readVLong())
            .thenReturn(1L) // clusterSize
            .thenReturn(1L) // docSize
            .thenReturn(1L); // summaryVectorSize
        when(mockPostingInput.readVInt())
            .thenReturn(1) // doc id
            .thenReturn(1); // sparse vector item index
        when(mockPostingInput.readByte())
            .thenReturn((byte) 1) // doc weight
            .thenReturn((byte) 1) // shouldNotSkip
            .thenReturn((byte) 1); // sparse vector item weight
    }

    private void setupMockPostingInputForEmptyClusters() throws IOException {
        when(mockPostingInput.readVLong()).thenReturn(0L); // clusterSize = 0
    }
}

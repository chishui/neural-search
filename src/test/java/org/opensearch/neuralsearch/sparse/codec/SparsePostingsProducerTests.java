/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.junit.After;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.util.Arrays;
import java.util.Iterator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;

public class SparsePostingsProducerTests extends AbstractSparseTestBase {

    private FieldsProducer mockDelegate;
    private SegmentReadState segmentReadState;
    private SparsePostingsProducer producer;
    private FieldInfo sparseFieldInfo;
    private SegmentInfo segmentInfo;
    private FieldInfos fieldInfos;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        mockDelegate = mock(FieldsProducer.class);

        // Setup segment info
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        // Setup field infos using real FieldInfo objects
        sparseFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        sparseFieldInfo.putAttribute(SPARSE_FIELD, "true");
        sparseFieldInfo.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "10");

        // Setup field infos
        fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.fieldInfo(sparseFieldInfo.name)).thenReturn(sparseFieldInfo);

        // Setup segment read state
        Directory mockDir = mock(Directory.class);
        segmentReadState = new SegmentReadState(mockDir, segmentInfo, fieldInfos, IOContext.DEFAULT);

        producer = new SparsePostingsProducer(mockDelegate, segmentReadState);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor() {
        SparsePostingsProducer localProducer = new SparsePostingsProducer(mockDelegate, segmentReadState);
        assertNotNull(localProducer);
        assertEquals(mockDelegate, localProducer.getDelegate());
        assertEquals(segmentReadState, localProducer.getState());
        assertNull(localProducer.getReader());
    }

    @SneakyThrows
    public void testClose_WithDelegate() {
        producer.close();

        verify(mockDelegate, times(1)).close();
    }

    @SneakyThrows
    public void testClose_WithNullDelegate() {
        SparsePostingsProducer producerWithNullDelegate = new SparsePostingsProducer(null, segmentReadState);

        // Should not throw exception
        producerWithNullDelegate.close();
    }

    @SneakyThrows
    public void testClose_WithReader() {
        // First call terms() to initialize reader
        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms(sparseFieldInfo.name)).thenReturn(mockTerms);

        producer.terms(sparseFieldInfo.name);
        assertNotNull(producer.getReader());

        producer.close();

        verify(mockDelegate, times(1)).close();
    }

    @SneakyThrows
    public void testCheckIntegrity() {
        producer.checkIntegrity();

        verify(mockDelegate, times(1)).checkIntegrity();
    }

    @SneakyThrows
    public void testIterator() {
        Iterator<String> mockIterator = Arrays.asList("field1", "field2").iterator();
        when(mockDelegate.iterator()).thenReturn(mockIterator);

        Iterator<String> result = producer.iterator();

        assertEquals(mockIterator, result);
        verify(mockDelegate, times(1)).iterator();
    }

    @SneakyThrows
    public void testTerms_SparseFieldBelowThreshold() {
        // Create segment info with low maxDoc
        SegmentInfo lowThresholdSegmentInfo = new SegmentInfo(
            segmentInfo.dir,
            segmentInfo.getVersion(),
            segmentInfo.getMinVersion(),
            "low_threshold_segment",
            3, // maxDoc below threshold (5)
            segmentInfo.getUseCompoundFile(),
            segmentInfo.getHasBlocks(),
            segmentInfo.getCodec(),
            segmentInfo.getDiagnostics(),
            segmentInfo.getId(),
            segmentInfo.getAttributes(),
            segmentInfo.getIndexSort()
        );

        SegmentReadState lowThresholdState = new SegmentReadState(
            segmentReadState.directory,
            lowThresholdSegmentInfo,
            fieldInfos,
            segmentReadState.context
        );

        SparsePostingsProducer lowThresholdProducer = new SparsePostingsProducer(mockDelegate, lowThresholdState);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms(sparseFieldInfo.name)).thenReturn(mockTerms);

        Terms result = lowThresholdProducer.terms(sparseFieldInfo.name);

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms(sparseFieldInfo.name);
        assertNull(lowThresholdProducer.getReader());

        lowThresholdProducer.close();
    }

    @SneakyThrows
    public void testTerms_SparseFieldAboveThreshold() {
        Terms result = producer.terms(sparseFieldInfo.name);

        assertNotNull(result);
        assertTrue(result instanceof SparseTerms);
        assertNotNull(producer.getReader());

        SparseTerms sparseTerms = (SparseTerms) result;
        InMemoryKey.IndexKey expectedKey = new InMemoryKey.IndexKey(segmentInfo, sparseFieldInfo);
        assertEquals(expectedKey, sparseTerms.getIndexKey());
    }

    @SneakyThrows
    public void testTerms_NullFieldInfo() {
        when(fieldInfos.fieldInfo("unknown_field")).thenReturn(null);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms("unknown_field")).thenReturn(mockTerms);

        Terms result = producer.terms("unknown_field");

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms("unknown_field");
    }

    @SneakyThrows
    public void testTerms_SparseFieldWithNullAttributes() {
        when(fieldInfos.fieldInfo("unknown_field")).thenReturn(null);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms("unknown_field")).thenReturn(mockTerms);

        Terms result = producer.terms("unknown_field");

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms("unknown_field");
    }

    @SneakyThrows
    public void testSize() {
        int result = producer.size();
        assertEquals(0, result);
    }

    @SneakyThrows
    public void testGetters() {
        assertEquals(mockDelegate, producer.getDelegate());
        assertEquals(segmentReadState, producer.getState());
    }
}

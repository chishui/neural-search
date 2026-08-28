/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * The native writer is the one path that outlives the ingest gate: a background merge of segments
 * written while the engine was on would otherwise reach the JNI library. These tests assert it is
 * skipped without touching {@link org.opensearch.neuralsearch.jni.NativeLibrary}, whose static
 * initializer loads the shared object.
 */
public class NativeDocValuesConsumerTests extends AbstractSparseTestBase {
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private MergeStateFacade mergeStateFacade;
    @Mock
    private FieldInfo nativeFieldInfo;
    @Mock
    private FieldInfo luceneFieldInfo;
    @Mock
    private SegmentInfo segmentInfo;

    private DocValuesProducer valuesProducer;
    private NativeDocValuesConsumer consumer;

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Map<String, String> nativeAttributes = new HashMap<>();
        nativeAttributes.put(SPARSE_FIELD, String.valueOf(true));
        nativeAttributes.put(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        when(nativeFieldInfo.attributes()).thenReturn(nativeAttributes);
        when(nativeFieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(SparseEngine.NATIVE.getName());
        when(nativeFieldInfo.getName()).thenReturn("native_field");

        Map<String, String> luceneAttributes = new HashMap<>();
        luceneAttributes.put(SPARSE_FIELD, String.valueOf(true));
        luceneAttributes.put(ENGINE_FIELD, SparseEngine.LUCENE.getName());
        when(luceneFieldInfo.attributes()).thenReturn(luceneAttributes);
        when(luceneFieldInfo.getAttribute(ENGINE_FIELD)).thenReturn(SparseEngine.LUCENE.getName());
        when(luceneFieldInfo.getName()).thenReturn("lucene_field");

        SegmentWriteState state = TestsPrepareUtils.prepareSegmentWriteState(segmentInfo);
        valuesProducer = mock(DocValuesProducer.class);
        consumer = new NativeDocValuesConsumer(state, mergeHelper);

        // Uninitialized settings means the dynamic gate sits at its default, off
        SparseSettings.reset();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        SparseSettings.reset();
        super.tearDown();
    }

    @SneakyThrows
    public void testAddBinaryFieldSkipsNativeFieldWhenDisabled() {
        consumer.addBinaryField(nativeFieldInfo, valuesProducer);

        // Never reads the values, so nothing reaches the native index writer
        verify(valuesProducer, never()).getBinary(any());
    }

    @SneakyThrows
    public void testMergeSkipsNativeFieldWhenDisabled() {
        consumer.merge(List.of(nativeFieldInfo), mergeStateFacade);

        verify(mergeHelper, never()).newSparseDocValuesReader(any());
    }

    @SneakyThrows
    public void testAddBinaryFieldIgnoresLuceneEngineField() {
        consumer.addBinaryField(luceneFieldInfo, valuesProducer);

        verify(valuesProducer, never()).getBinary(any());
    }

    @SneakyThrows
    public void testMergeIgnoresLuceneEngineField() {
        consumer.merge(List.of(luceneFieldInfo), mergeStateFacade);

        verify(mergeHelper, never()).newSparseDocValuesReader(any());
    }
}

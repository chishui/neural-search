/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import static org.opensearch.neuralsearch.sparse.SparseTokensField.SPARSE_FIELD;

public class MergeHelperTests extends AbstractSparseTestBase {
    private static MergeState mergeState;
    private static FieldInfo fieldInfo;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mergeState = TestsPrepareUtils.prepareMergeState(false);
        fieldInfo = mergeState.mergeFieldInfos.iterator().next();
        fieldInfo.putAttribute(SPARSE_FIELD, "true");

        SparseBinaryDocValuesPassThrough mockBinaryDocValues = TestsPrepareUtils.prepareSparseBinaryDocValuesPassThrough();
        DocValuesProducer docValuesProducer = TestsPrepareUtils.prepareDocValuesProducer(mockBinaryDocValues);
        mergeState.docValuesProducers[0] = docValuesProducer;

    }

    public void testClearInMemoryData_withValidSparseField_callsConsumer() throws IOException {
        List<InMemoryKey.IndexKey> capturedKeys = new ArrayList<>();
        Consumer<InMemoryKey.IndexKey> consumer = capturedKeys::add;

        // Execute
        MergeHelper.clearInMemoryData(mergeState, fieldInfo, consumer);

        // Verify
        assertEquals("Consumer should be called once", 1, capturedKeys.size());
        assertNotNull("Captured key should not be null", capturedKeys.get(0));
    }

    public void testClearInMemoryData_withNullFieldInfo_processesAllSparseFields() throws IOException {
        // Create consumer to capture calls
        List<InMemoryKey.IndexKey> capturedKeys = new ArrayList<>();
        Consumer<InMemoryKey.IndexKey> consumer = capturedKeys::add;

        // Execute with null fieldInfo (should process all sparse fields)
        MergeHelper.clearInMemoryData(mergeState, null, consumer);

        // Verify
        assertEquals("Consumer should be called for sparse field", 1, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseFieldInfo_processesAllSparseFields() throws IOException {
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(false);
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        SparseBinaryDocValuesPassThrough mockBinaryDocValues = TestsPrepareUtils.prepareSparseBinaryDocValuesPassThrough();
        DocValuesProducer docValuesProducer = TestsPrepareUtils.prepareDocValuesProducer(mockBinaryDocValues);
        mergeState.docValuesProducers[0] = docValuesProducer;
        // Create consumer to capture calls
        List<InMemoryKey.IndexKey> capturedKeys = new ArrayList<>();
        Consumer<InMemoryKey.IndexKey> consumer = capturedKeys::add;

        // Execute with null fieldInfo (should process all sparse fields)
        MergeHelper.clearInMemoryData(mergeState, fieldInfo, consumer);

        // Verify
        assertEquals("Consumer should NOT be called for sparse field", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseBinaryDocValues_skipsField() throws IOException {
        BinaryDocValues mockBinaryDocValues = TestsPrepareUtils.prepareBinaryDocValues();
        DocValuesProducer docValuesProducer = TestsPrepareUtils.prepareDocValuesProducer(mockBinaryDocValues);
        mergeState.docValuesProducers[0] = docValuesProducer;

        // Create consumer to capture calls
        List<InMemoryKey.IndexKey> capturedKeys = new ArrayList<>();
        Consumer<InMemoryKey.IndexKey> consumer = capturedKeys::add;

        // Execute
        MergeHelper.clearInMemoryData(mergeState, fieldInfo, consumer);

        // Verify consumer was not called
        assertEquals("Consumer should NOT be called for sparse field", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withEmptyMergeState_doesNotCallConsumer() throws IOException {
        // Setup merge state with no producers
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(true); // Empty maxDocs

        // Create consumer to capture calls
        List<InMemoryKey.IndexKey> capturedKeys = new ArrayList<>();
        Consumer<InMemoryKey.IndexKey> consumer = capturedKeys::add;

        // Execute
        MergeHelper.clearInMemoryData(mergeState, fieldInfo, consumer);

        // Verify consumer was not called
        assertEquals("Consumer should NOT be called for sparse field", 0, capturedKeys.size());
    }

}

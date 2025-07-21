/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SparseBinaryDocValuesTests extends AbstractSparseTestBase {

    private DocIDMerger<BinaryDocValuesSub> docIDMerger;
    private BinaryDocValuesSub binaryDocValuesSub;
    private BinaryDocValues binaryDocValues;
    private SparseBinaryDocValues sparseBinaryDocValues;

    @Before
    public void setup() {
        docIDMerger = mock(DocIDMerger.class);
        binaryDocValuesSub = mock(BinaryDocValuesSub.class);
        binaryDocValues = mock(BinaryDocValues.class);
        sparseBinaryDocValues = new SparseBinaryDocValues(docIDMerger);
    }

    public void testDocID() {
        assertEquals(-1, sparseBinaryDocValues.docID());
    }

    public void testNextDoc_WithNullCurrent() throws IOException {
        when(docIDMerger.next()).thenReturn(null);

        int result = sparseBinaryDocValues.nextDoc();

        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, result);
        assertEquals(SparseBinaryDocValues.NO_MORE_DOCS, sparseBinaryDocValues.docID());
    }

    public void testAdvance() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.advance(5));
    }

    public void testAdvanceExact() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.advanceExact(5));
    }

    public void testCost() {
        expectThrows(UnsupportedOperationException.class, () -> sparseBinaryDocValues.cost());
    }

    public void testBinaryValue() throws IOException {
        BytesRef bytesRef = new BytesRef("test");
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getValues()).thenReturn(binaryDocValues);
        when(binaryDocValues.binaryValue()).thenReturn(bytesRef);

        sparseBinaryDocValues.nextDoc();
        BytesRef result = sparseBinaryDocValues.binaryValue();

        assertEquals(bytesRef, result);
    }

    public void testCachedSparseVector_WithNullCurrent() throws IOException {
        assertNull(sparseBinaryDocValues.cachedSparseVector());
    }

    public void testCachedSparseVector_WithNullKey() throws IOException {
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        when(binaryDocValuesSub.getKey()).thenReturn(null);

        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertNull(result);
    }

    public void testCachedSparseVector_WithNonNullKey() throws IOException {
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        when(docIDMerger.next()).thenReturn(binaryDocValuesSub);
        InMemoryKey.IndexKey testKey = new InMemoryKey.IndexKey(segmentInfo, fieldInfo);
        SparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.getOrCreate(testKey, 10);
        when(binaryDocValuesSub.getKey()).thenReturn(testKey);
        sparseBinaryDocValues.nextDoc();
        SparseVector result = sparseBinaryDocValues.cachedSparseVector();

        assertNull(result); // index retrieved by this key is null, so it should return null
    }

    public void testSetTotalLiveDocs() {
        SparseBinaryDocValues result = sparseBinaryDocValues.setTotalLiveDocs(100L);

        assertEquals(100L, sparseBinaryDocValues.getTotalLiveDocs());
        assertEquals(sparseBinaryDocValues, result);
    }
}

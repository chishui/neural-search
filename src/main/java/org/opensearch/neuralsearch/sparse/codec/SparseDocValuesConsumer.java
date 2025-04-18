/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;

public class SparseDocValuesConsumer extends DocValuesConsumer {
    private final DocValuesConsumer delegate;
    private final SegmentWriteState state;

    public SparseDocValuesConsumer(SegmentWriteState state, DocValuesConsumer delegate) {
        super();
        this.delegate = delegate;
        this.state = state;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addBinaryField(field, valuesProducer);
        // check field is the sparse field, otherwise return
        if (!SparseTokensField.isSparseField(field)) {
            return;
        }
        addBinary(field, valuesProducer, false);
    }

    private void addBinary(FieldInfo field, DocValuesProducer valuesProducer, boolean isMerge) throws IOException {
        if (isMerge) {
            SparseDocValuesReader reader = (SparseDocValuesReader) valuesProducer;
            for (DocValuesProducer producer : reader.getMergeState().docValuesProducers) {
                SparseDocValuesProducer sparseDocValuesProducer = (SparseDocValuesProducer) producer;
                InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(sparseDocValuesProducer.getState().segmentInfo, field);
                SparseVectorForwardIndex.removeIndex(key);
            }
        }
        BinaryDocValues binaryDocValues = valuesProducer.getBinary(field);
        int docId;
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(this.state.segmentInfo, field);
        SparseVectorForwardIndex index = SparseVectorForwardIndex.getOrCreate(key);
        SparseVectorForwardIndex.SparseVectorForwardIndexWriter writer = index.getForwardIndexWriter();
        while ((docId = binaryDocValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytesRef = binaryDocValues.binaryValue();
            writer.write(docId, bytesRef);
        }
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        this.delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }

    @Override
    public void merge(MergeState mergeState) {
        try {
            this.delegate.merge(mergeState);
            assert mergeState != null;
            assert mergeState.mergeFieldInfos != null;
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                DocValuesType type = fieldInfo.getDocValuesType();
                if (type == DocValuesType.BINARY && SparseTokensField.isSparseField(fieldInfo)) {
                    addBinary(fieldInfo, new SparseDocValuesReader(mergeState), true);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

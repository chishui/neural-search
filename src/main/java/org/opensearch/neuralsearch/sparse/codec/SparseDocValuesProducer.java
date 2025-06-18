/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;

/**
 *
 */
@Log4j2
public class SparseDocValuesProducer extends DocValuesProducer {
    private final DocValuesProducer delegate;
    @Getter
    private final SegmentReadState state;

    public SparseDocValuesProducer(SegmentReadState state, DocValuesProducer delegate) {
        super();
        this.state = state;
        this.delegate = delegate;
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return this.delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        BinaryDocValues binaryDocValues = this.delegate.getBinary(field);
        if (SparseTokensField.isSparseField(field)) {
            readBinary(field, binaryDocValues);
        }
        return new SparseBinaryDocValuesPassThrough(binaryDocValues, this.state.segmentInfo);
    }

    private void readBinary(FieldInfo field, BinaryDocValues binaryDocValues) throws IOException {
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(this.state.segmentInfo, field);
        int docCount = this.state.segmentInfo.maxDoc();
        InMemorySparseVectorForwardIndex forwardIndex = InMemorySparseVectorForwardIndex.get(key);
        if (forwardIndex == null) {
            log.info("Forward index is null. Need to load it from binary doc values");
            SparseVectorForwardIndex.SparseVectorForwardIndexWriter writer = InMemorySparseVectorForwardIndex.getOrCreate(key, docCount)
                .getForwardIndexWriter();
            if (writer == null) {
                throw new IllegalStateException("Forward index writer is null");
            }
            int docId = binaryDocValues.nextDoc();
            while (docId != DocIdSetIterator.NO_MORE_DOCS) {
                BytesRef bytesRef = binaryDocValues.binaryValue();
                writer.write(docId, bytesRef);
                docId = binaryDocValues.nextDoc();
            }
        }
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return this.delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return this.delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return this.delegate.getSortedSet(field);
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        return this.delegate.getSkipper(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.delegate.checkIntegrity();
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.DefaultNativeIndexWriter;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.List;

@AllArgsConstructor
public class NativeDocValuesConsumer extends SparseVectorBinaryConsumer {
    private final SegmentWriteState state;
    private final MergeHelper mergeHelper;

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        if (!SparseVectorField.isSparseField(field) || SparseFieldUtils.getSparseEngine(field) != SparseEngine.NATIVE) {
            return;
        }
        DefaultNativeIndexWriter indexWriter = new DefaultNativeIndexWriter(state, field);
        BinaryDocValues binaryDocValues = valuesProducer.getBinary(field);
        indexWriter.WriteIndex(binaryDocValues);
    }

    @Override
    public void merge(List<FieldInfo> fieldInfos, MergeStateFacade mergeStateFacade) throws IOException {
        for (FieldInfo fieldInfo : fieldInfos) {
            if (!SparseVectorField.isSparseField(fieldInfo) || SparseFieldUtils.getSparseEngine(fieldInfo) != SparseEngine.NATIVE) {
                continue;
            }
            DefaultNativeIndexWriter indexWriter = new DefaultNativeIndexWriter(state, fieldInfo);
            SparseDocValuesReader sparseDocValuesReader = mergeHelper.newSparseDocValuesReader(mergeStateFacade);
            indexWriter.WriteIndex(sparseDocValuesReader.getBinary(fieldInfo));
        }
    }
}

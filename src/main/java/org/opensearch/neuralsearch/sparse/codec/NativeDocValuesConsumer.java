/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.DefaultNativeIndexWriter;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.List;

@Log4j2
@AllArgsConstructor
public class NativeDocValuesConsumer extends SparseVectorBinaryConsumer {
    private final SegmentWriteState state;
    private final MergeHelper mergeHelper;

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        if (shouldSkip(field)) {
            return;
        }
        DefaultNativeIndexWriter indexWriter = new DefaultNativeIndexWriter(state, field);
        BinaryDocValues binaryDocValues = valuesProducer.getBinary(field);
        indexWriter.WriteIndex(binaryDocValues);
    }

    @Override
    public void merge(List<FieldInfo> fieldInfos, MergeStateFacade mergeStateFacade) throws IOException {
        for (FieldInfo fieldInfo : fieldInfos) {
            if (shouldSkip(fieldInfo)) {
                continue;
            }
            DefaultNativeIndexWriter indexWriter = new DefaultNativeIndexWriter(state, fieldInfo);
            SparseDocValuesReader sparseDocValuesReader = mergeHelper.newSparseDocValuesReader(mergeStateFacade);
            indexWriter.WriteIndex(sparseDocValuesReader.getBinary(fieldInfo));
        }
    }

    private boolean shouldSkip(FieldInfo field) {
        if (!SparseVectorField.isSparseField(field) || SparseFieldUtils.getSparseEngine(field) != SparseEngine.NATIVE) {
            return true;
        }
        // A merge of pre-existing native segments is the one write that outlives the ingest gate, and
        // it would load the JNI library. Skip it: the raw vectors still reach disk through the
        // delegate consumer, so re-enabling and force-merging rebuilds the native index.
        if (SparseSettings.state().isNativeEngineEnabled() == false) {
            log.warn(
                "Skipping native sparse index for field [{}] in segment [{}]: {}",
                field.getName(),
                state.segmentInfo.name,
                SparseSettings.NATIVE_ENGINE_DISABLED_REASON
            );
            return true;
        }
        return false;
    }
}

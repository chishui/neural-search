/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.NonNull;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;

import java.io.IOException;
import java.util.function.Consumer;

public class MergeHelper {
    public static void clearInMemoryData(
        @NonNull MergeStateFacade mergeState,
        @Nullable FieldInfo fieldInfo,
        @NonNull Consumer<InMemoryKey.IndexKey> consumer
    ) throws IOException {
        for (DocValuesProducer producer : mergeState.getDocValuesProducers()) {
            for (FieldInfo field : mergeState.getMergeFieldInfos()) {
                boolean isNotSparse = !SparseTokensField.isSparseField(field);
                boolean fieldInfoMatched = fieldInfo == null || field == fieldInfo;
                if (isNotSparse || fieldInfoMatched) {
                    continue;
                }
                BinaryDocValues binaryDocValues = producer.getBinary(field);
                if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough binaryDocValuesPassThrough)) {
                    continue;
                }
                InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(binaryDocValuesPassThrough.getSegmentInfo(), field);
                consumer.accept(key);
            }
        }
    }
}

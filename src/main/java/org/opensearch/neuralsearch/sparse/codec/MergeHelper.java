/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensField;

import java.io.IOException;
import java.util.function.Consumer;

public class MergeHelper {
    public static void clearCacheData(
        @NonNull MergeStateFacade mergeState,
        @Nullable FieldInfo fieldInfo,
        @NonNull Consumer<CacheKey> consumer
    ) throws IOException {
        for (DocValuesProducer producer : mergeState.getDocValuesProducers()) {
            for (FieldInfo field : mergeState.getMergeFieldInfos()) {
                boolean isNotSparse = !SparseTokensField.isSparseField(field);
                boolean fieldInfoMisMatched = fieldInfo != null && field != fieldInfo;
                if (isNotSparse || fieldInfoMisMatched) {
                    continue;
                }
                BinaryDocValues binaryDocValues = producer.getBinary(field);
                if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough binaryDocValuesPassThrough)) {
                    continue;
                }
                CacheKey key = new CacheKey(binaryDocValuesPassThrough.getSegmentInfo(), field);
                consumer.accept(key);
            }
        }
    }
}

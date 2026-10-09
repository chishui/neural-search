/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.apache.lucene.index.FieldInfo;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

/**
 * The search-time knobs of a seismic sparse field, derived once from the query and the mapping.
 *
 * <p>Shared by the two scorers that search a native engine file -- {@link NativeIndexScorer} through
 * JNI and {@link DiskSeismicSqScorer} through an {@link org.apache.lucene.store.IndexInput} -- so the
 * two cannot drift into searching the same file with different parameters, which would show up as a
 * recall difference rather than as an error.
 *
 * @param cut        how many of the query's heaviest tokens contribute candidate blocks
 * @param heapFactor the user-facing recall/latency knob
 * @param kPrime     the block budget {@code heapFactor} maps to, which is what a {@code per_block}
 *                   (disk-resident) index takes instead
 * @param vmin       low end of the range the query is quantized over
 * @param vmax       high end of that range
 */
record SeismicQueryParameters(int cut, float heapFactor, int kPrime, float vmin, float vmax) {

    static SeismicQueryParameters from(SparseQueryContext queryContext, FieldInfo fieldInfo) {
        return new SeismicQueryParameters(
            queryContext.getTokens().size(),
            queryContext.getHeapFactor(),
            DiskSeismicKPrime.fromHeapFactor(queryContext.getHeapFactor()),
            // The query is quantized over the search ceiling rather than the index's ingest one, so a
            // query weight above the ingest ceiling is not clamped to the top code.
            0.0f,
            ByteQuantizationUtil.getCeilingValueSearch(fieldInfo)
        );
    }
}

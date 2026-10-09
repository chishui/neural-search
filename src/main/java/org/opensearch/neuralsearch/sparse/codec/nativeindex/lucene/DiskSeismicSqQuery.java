/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

/**
 * One search against a {@link DiskSeismicSqIndexReader}, holding the same knobs the native path puts
 * into its {@code methodParameters} map.
 *
 * @param tokens       query token ids
 * @param weights      query weights, aligned with {@code tokens}
 * @param k            hits to return
 * @param cut          how many of the query's heaviest tokens contribute candidate blocks
 * @param kPrime       block budget: how many of the highest-scoring blocks are read
 * @param vmin         low end of the range the query is quantized over (the search ceiling, not the
 *                     index's ingest one, so a weight above the ingest ceiling is not clamped to the
 *                     top code)
 * @param vmax         high end of that range
 * @param filterDocIds Lucene doc ids the search is restricted to, ascending, or null for none. Taken
 *                     as {@code long} rather than {@code int} so the same array a caller builds for
 *                     the native path can be passed straight through, unconverted. A
 *                     filter of at most {@code k} documents makes the search exact over exactly
 *                     those, as the native path does -- a caller that wants every member returned
 *                     depends on it.
 */
public record DiskSeismicSqQuery(int[] tokens, float[] weights, int k, int cut, int kPrime, float vmin, float vmax, long[] filterDocIds) {
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.SegmentNativeIndex;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Scores one segment against the native engine index, through JNI.
 *
 * nsparse has no cursor: the whole top-k comes back from a {@code queryIndex} call in the
 * constructor, and this scorer then just replays those hits in doc order. So unlike the Lucene
 * path, k is decided before iteration and the collector cannot make the query stop early. The fetch
 * itself -- filters down as a candidate set, deletions dropped afterwards -- is
 * {@link UpfrontSparseSearch}, shared with {@link DiskSeismicSqScorer}, which does the same over a
 * Java reader of the same file.
 */
@Log4j2
public class NativeIndexScorer extends Scorer {
    /**
     * nsparse's selector kind for the filter ids: 0 builds a SetIDSelector, 1 an ArrayIDSelector.
     */
    private static final int FILTER_IDS_TYPE_SET = 0;

    private final ResultsDocValueIterator<Float> resultsIterator;
    /**
     * nsparse decodes its own quantization, so unlike the Lucene path there is no rescaling to
     * fold the boost into -- it multiplies the returned score and nothing else.
     */
    private final float boost;

    /**
     * Creates scorer with upfront search results and optional filtering.
     */
    public NativeIndexScorer(
        FieldInfo fieldInfo,
        SparseQueryContext sparseQueryContext,
        Map<Integer, Float> rawQueryVector,
        LeafReader leafReader,
        SegmentInfo segmentInfo,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        float boost
    ) throws IOException {
        this.boost = boost;
        StopWatch searchUpfrontStopWatch = StopWatch.createStarted();
        List<Pair<Integer, Float>> results = searchUpfront(
            sparseQueryContext,
            fieldInfo,
            rawQueryVector,
            sparseQueryContext.getK(),
            acceptedDocs,
            filterBitSetIterator,
            leafReader,
            segmentInfo
        );
        int maxDoc = segmentInfo.maxDoc();
        for (Pair<Integer, Float> result : results) {
            if (result.getKey() >= maxDoc) {
                log.error("docId: {}, maxDoc: {}", result.getKey(), maxDoc);
            }
        }
        searchUpfrontStopWatch.stop();
        log.debug("searchUpfront took {} ms", searchUpfrontStopWatch.getTime(TimeUnit.MILLISECONDS));
        resultsIterator = new ResultsDocValueIterator<>(results);
    }

    private List<Pair<Integer, Float>> searchUpfront(
        SparseQueryContext sparseQueryContext,
        FieldInfo fieldInfo,
        Map<Integer, Float> rawQueryVector,
        int resultSize,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        LeafReader leafReader,
        SegmentInfo segmentInfo
    ) throws IOException {
        int[] tokens = Ints.toArray(rawQueryVector.keySet());
        float[] weights = Floats.toArray(rawQueryVector.values());
        Map<String, Object> searchParameters = buildSearchParameters(sparseQueryContext, segmentInfo, fieldInfo);

        StopWatch loadIndexStopWatch = StopWatch.createStarted();
        try (SegmentNativeIndex nativeIndex = SegmentNativeIndex.open(leafReader, segmentInfo, fieldInfo.getName())) {
            long indexAddress = nativeIndex.address();
            loadIndexStopWatch.stop();
            log.debug("loadIndex took {} ms", loadIndexStopWatch.getTime(TimeUnit.MILLISECONDS));

            return UpfrontSparseSearch.execute(
                resultSize,
                segmentInfo.maxDoc(),
                acceptedDocs,
                filterBitSetIterator,
                (fetchSize, docsIds) -> queryNative(indexAddress, tokens, weights, fetchSize, searchParameters, docsIds)
            );
        } catch (Exception e) {
            log.error("search parameters: {}", searchParameters);
            throw e;
        }
    }

    private SparseQueryResult[] queryNative(
        long indexAddress,
        int[] tokens,
        float[] weights,
        int fetchSize,
        Map<String, Object> searchParameters,
        long[] docsIds
    ) {
        if (docsIds == null) {
            return NativeLibrary.queryIndex(indexAddress, tokens, weights, fetchSize, searchParameters);
        }
        return NativeLibrary.queryIndexWithFilter(indexAddress, tokens, weights, fetchSize, searchParameters, docsIds, FILTER_IDS_TYPE_SET);
    }

    private static Map<String, Object> buildSearchParameters(
        SparseQueryContext sparseQueryContext,
        SegmentInfo segmentInfo,
        FieldInfo fieldInfo
    ) {
        Map<String, Object> searchParameters = new HashMap<>();
        if (PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
            SeismicQueryParameters parameters = SeismicQueryParameters.from(sparseQueryContext, fieldInfo);
            searchParameters.put("cut", parameters.cut());
            searchParameters.put("heap_factor", parameters.heapFactor());
            // vmin/vmax select a quantized SearchParameters subtype, which encodes the query at
            // the search ceiling instead of the index's ingest one. Without them the quantized
            // index falls back to its own doc-side quantizer, clamping every query weight
            // above the ingest ceiling to the top code.
            searchParameters.put("vmin", parameters.vmin());
            searchParameters.put("vmax", parameters.vmax());
            if (SparseFieldUtils.getSparseForwardIndex(fieldInfo) == SparseForwardIndex.PER_BLOCK) {
                // k_prime is what makes the range above land on DiskSeismicSQSearchParameters, the
                // only subtype a disk_seismic_sq index reads a query range from, so it must always
                // be present for a per_block field. It is also the recall/latency budget: derive it
                // from the query's heap_factor so a single user-facing knob drives both the disk and
                // the in-memory paths, instead of the native default that ignored heap_factor.
                searchParameters.put("k_prime", parameters.kPrime());
            }
        }
        return searchParameters;
    }

    @Override
    public int docID() {
        return resultsIterator.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return resultsIterator;
    }

    /**
     * Returns maximum possible score up to given document ID.
     */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    /**
     * Computes score for current document using similarity scorer.
     */
    @Override
    public float score() throws IOException {
        return boost * resultsIterator.score();
    }
}

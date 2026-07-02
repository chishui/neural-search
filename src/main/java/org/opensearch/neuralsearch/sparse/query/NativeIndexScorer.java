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
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.commons.lang3.time.StopWatch;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.cache.NativeCacheManager;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class NativeIndexScorer extends Scorer {
    private final ResultsDocValueIterator<Float> resultsIterator;

    /**
     * Creates scorer with upfront search results and optional filtering.
     */
    public NativeIndexScorer(
        FieldInfo fieldInfo,
        SparseQueryContext sparseQueryContext,
        Map<Integer, Float> rawQueryVector,
        SegmentInfo segmentInfo,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator
    ) throws IOException {
        StopWatch searchUpfrontStopWatch = StopWatch.createStarted();
        List<Pair<Integer, Float>> results = searchUpfront(
            sparseQueryContext,
            fieldInfo,
            rawQueryVector,
            sparseQueryContext.getK(),
            acceptedDocs,
            filterBitSetIterator,
            segmentInfo
        );
        int maxDoc = segmentInfo.maxDoc();
        for (Pair<Integer, Float> result : results) {
            if (result.getKey() >= maxDoc) {
                log.error("docId: {}, maxDoc: {}", result.getKey(), maxDoc);
            }
        }
        searchUpfrontStopWatch.stop();
        log.debug("searchUpfront took {} ms", searchUpfrontStopWatch.getTime(java.util.concurrent.TimeUnit.MILLISECONDS));
        resultsIterator = new ResultsDocValueIterator<>(results);
    }

    private List<Pair<Integer, Float>> searchUpfront(
        SparseQueryContext sparseQueryContext,
        FieldInfo fieldInfo,
        Map<Integer, Float> rawQueryVector,
        int resultSize,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator,
        SegmentInfo segmentInfo
    ) throws IOException {
        int tokens[] = Ints.toArray(rawQueryVector.keySet());
        float weights[] = Floats.toArray(rawQueryVector.values());
        Map<String, Object> searchParameters = buildSearchParameters(sparseQueryContext, segmentInfo, fieldInfo);
        SparseQueryResult results[];

        StopWatch loadIndexStopWatch = StopWatch.createStarted();
        try (NativeCacheManager.IndexLease lease = loadIndex(segmentInfo, fieldInfo.getName())) {
            loadIndexStopWatch.stop();
            log.debug("loadIndex took {} ms", loadIndexStopWatch.getTime(java.util.concurrent.TimeUnit.MILLISECONDS));

            long indexAddress = lease.getIndexAddress();

            long[] docsIds = null;
            if (acceptedDocs != null || filterBitSetIterator != null) {
                docsIds = constructFilterList(segmentInfo, acceptedDocs, filterBitSetIterator);
            }

            if (docsIds == null) {
                results = NativeLibrary.queryIndex(indexAddress, tokens, weights, resultSize, searchParameters);
            } else {
                results = NativeLibrary.queryIndexWithFilter(indexAddress, tokens, weights, resultSize, searchParameters, docsIds, 0);
            }
        } catch (Exception e) {
            log.error("search parameters: {}", searchParameters);
            throw e;
        }
        return java.util.Arrays.stream(results).map(result -> Pair.of(result.getId(), result.getScore())).toList();
    }

    private long[] constructFilterList(SegmentInfo segmentInfo, Bits acceptedDocs, BitSetIterator filterBitSetIterator) throws IOException {
        DocIdSetIterator iterator;
        if (filterBitSetIterator != null) {
            iterator = (acceptedDocs != null) ? new FilteredBitSetIterator(filterBitSetIterator, acceptedDocs) : filterBitSetIterator;
        } else {
            // acceptedDocs only (liveDocs) — iterate all docs, skip deleted
            iterator = new LiveDocsIterator(acceptedDocs, segmentInfo.maxDoc());
        }

        List<Long> docIds = new java.util.ArrayList<>();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docIds.add((long) doc);
        }
        return docIds.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Iterates a BitSetIterator while also checking acceptedDocs (liveDocs).
     */
    private static class FilteredBitSetIterator extends DocIdSetIterator {
        private final BitSetIterator delegate;
        private final Bits acceptedDocs;

        FilteredBitSetIterator(BitSetIterator delegate, Bits acceptedDocs) {
            this.delegate = delegate;
            this.acceptedDocs = acceptedDocs;
        }

        @Override
        public int docID() {
            return delegate.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            int doc;
            while ((doc = delegate.nextDoc()) != NO_MORE_DOCS) {
                if (acceptedDocs.get(doc)) {
                    return doc;
                }
            }
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = delegate.advance(target);
            if (doc == NO_MORE_DOCS) return NO_MORE_DOCS;
            if (acceptedDocs.get(doc)) return doc;
            return nextDoc();
        }

        @Override
        public long cost() {
            return delegate.cost();
        }
    }

    /**
     * Iterates all doc IDs [0, maxDoc) that are accepted by liveDocs.
     */
    private static class LiveDocsIterator extends DocIdSetIterator {
        private final Bits liveDocs;
        private final int maxDoc;
        private int doc = -1;

        LiveDocsIterator(Bits liveDocs, int maxDoc) {
            this.liveDocs = liveDocs;
            this.maxDoc = maxDoc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            doc++;
            while (doc < maxDoc) {
                if (liveDocs.get(doc)) return doc;
                doc++;
            }
            return doc = NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            doc = target;
            if (doc >= maxDoc) return doc = NO_MORE_DOCS;
            if (liveDocs.get(doc)) return doc;
            return nextDoc();
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }

    private Map<String, Object> buildSearchParameters(SparseQueryContext sparseQueryContext, SegmentInfo segmentInfo, FieldInfo fieldInfo) {
        Map<String, Object> searchParameters = new HashMap<>();
        if (PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo)) {
            float ingestCeilingValue = ByteQuantizationUtil.getCeilingValueIngest(fieldInfo);
            float searchCeilingValue = ByteQuantizationUtil.getCeilingValueSearch(fieldInfo);
            searchParameters.put("cut", sparseQueryContext.getTokens().size());
            searchParameters.put("heap_factor", sparseQueryContext.getHeapFactor());
            if (ingestCeilingValue != searchCeilingValue) {
                searchParameters.put("vmin", 0);
                searchParameters.put("vmax", searchCeilingValue);
            }
        }
        return searchParameters;
    }

    private NativeCacheManager.IndexLease loadIndex(SegmentInfo segmentInfo, String fieldName) throws IOException {
        Directory directory = segmentInfo.dir;
        List<String> engineFileNames = CodecUtils.getEngineFiles(SparseEngine.NATIVE.extension(), fieldName, segmentInfo);
        String cacheKey = NativeCacheManager.constructCacheKey(engineFileNames.get(0), segmentInfo);
        return NativeCacheManager.instance().acquireIndex(cacheKey, directory);
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
        return resultsIterator.score();
    }
}

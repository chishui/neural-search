/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene.DiskSeismicSqIndexReader;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene.DiskSeismicSqQuery;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene.DiskSeismicSqSearcher;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene.SegmentDiskSeismicSqIndex;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Scores one segment of a {@code per_block} sparse field by reading its native engine file through a
 * Lucene {@link org.apache.lucene.store.IndexInput}, rather than by asking nsparse to {@code mmap} it.
 *
 * <p>Same search, same file, same parameters as {@link NativeIndexScorer} -- the top-k arrives whole
 * from one call, filters go down as a candidate set and deletions are dropped afterwards, all through
 * {@link UpfrontSparseSearch}. What differs is who parses the file: {@link DiskSeismicSqSearcher}
 * does, in Java, so the bytes can come from a directory that has no filesystem path whose contents are
 * the index -- one that decrypts or fetches in {@code openInput}. That is also the case the JNI path
 * cannot serve at all, since it needs a path and {@code disk_seismic_sq} has no copying reader.
 *
 * <p>Only usable where the segment's engine file actually holds a {@code disk_seismic_sq} index: a
 * {@code per_block} field whose segment is at or above its {@code approximate_threshold}. Below the
 * threshold the writer builds an nsparse {@code inverted} index, which this reader does not parse.
 * {@link SparseQueryWeight} makes that check before choosing this scorer.
 */
@Log4j2
public class DiskSeismicSqScorer extends Scorer {

    private final ResultsDocValueIterator<Float> resultsIterator;
    /**
     * The reader decodes its own quantization, so as on the native path there is no rescaling to fold
     * the boost into -- it multiplies the returned score and nothing else.
     */
    private final float boost;

    public DiskSeismicSqScorer(
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
        StopWatch stopWatch = StopWatch.createStarted();
        List<Pair<Integer, Float>> results = searchUpfront(
            fieldInfo,
            sparseQueryContext,
            rawQueryVector,
            leafReader,
            segmentInfo,
            acceptedDocs,
            filterBitSetIterator
        );
        stopWatch.stop();
        log.debug("IndexInput sparse search took {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
        this.resultsIterator = new ResultsDocValueIterator<>(results);
    }

    private static List<Pair<Integer, Float>> searchUpfront(
        FieldInfo fieldInfo,
        SparseQueryContext sparseQueryContext,
        Map<Integer, Float> rawQueryVector,
        LeafReader leafReader,
        SegmentInfo segmentInfo,
        Bits acceptedDocs,
        BitSetIterator filterBitSetIterator
    ) throws IOException {
        int[] tokens = Ints.toArray(rawQueryVector.keySet());
        float[] weights = Floats.toArray(rawQueryVector.values());
        SeismicQueryParameters parameters = SeismicQueryParameters.from(sparseQueryContext, fieldInfo);

        try (SegmentDiskSeismicSqIndex index = SegmentDiskSeismicSqIndex.open(leafReader, segmentInfo, fieldInfo.getName())) {
            DiskSeismicSqIndexReader reader = index.reader();
            if (reader.isEmpty()) {
                return List.of();
            }
            // One searcher per query: it owns the cloned IndexInput and the buffers a query reuses
            // across its blocks, neither of which can be shared with a concurrent search.
            DiskSeismicSqSearcher searcher = new DiskSeismicSqSearcher(reader);
            return UpfrontSparseSearch.execute(
                sparseQueryContext.getK(),
                segmentInfo.maxDoc(),
                acceptedDocs,
                filterBitSetIterator,
                (fetchSize, filterDocIds) -> searcher.search(
                    new DiskSeismicSqQuery(
                        tokens,
                        weights,
                        fetchSize,
                        parameters.cut(),
                        parameters.kPrime(),
                        parameters.vmin(),
                        parameters.vmax(),
                        filterDocIds
                    )
                )
            );
        }
    }

    @Override
    public int docID() {
        return resultsIterator.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
        return resultsIterator;
    }

    /** No upper bound, as on every other sparse path: the top-k is fixed before iteration. */
    @Override
    public float getMaxScore(int upTo) throws IOException {
        return 0;
    }

    @Override
    public float score() throws IOException {
        return boost * resultsIterator.score();
    }
}

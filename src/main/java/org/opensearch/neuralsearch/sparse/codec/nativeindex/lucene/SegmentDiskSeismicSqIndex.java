/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link DiskSeismicSqIndexReader} for one field of one segment, parsed on first query and closed
 * when the segment core closes.
 *
 * <p>The counterpart of {@link org.opensearch.neuralsearch.sparse.codec.nativeindex.SegmentNativeIndex}
 * for the Lucene-side reader, and cached for the same reason: parsing the id map, the cluster
 * summaries and the block directory is per-segment work that every query on that segment would
 * otherwise repeat. What is cached is small and bounded -- the summaries and the directory, not the
 * corpus -- and the block payloads stay on disk.
 *
 * <p>Lifetime is the segment core's, so a reader handed to a query cannot be closed underneath it and
 * an NRT refresh reuses the parse instead of redoing it. Unlike the native handle, this holds an open
 * {@code IndexInput}, which is what the core-close listener releases.
 */
@Log4j2
public final class SegmentDiskSeismicSqIndex implements Closeable {

    private static final ConcurrentMap<Key, SegmentDiskSeismicSqIndex> OPEN_INDICES = new ConcurrentHashMap<>();

    /** A segment core can carry several sparse fields, each with its own engine file. */
    private record Key(IndexReader.CacheKey core, String fieldName) {
    }

    private final SegmentInfo segmentInfo;
    private final String fileName;
    private final boolean coreScoped;
    private DiskSeismicSqIndexReader reader;
    private boolean closed;

    private SegmentDiskSeismicSqIndex(SegmentInfo segmentInfo, String fileName, boolean coreScoped) {
        this.segmentInfo = segmentInfo;
        this.fileName = fileName;
        this.coreScoped = coreScoped;
    }

    /**
     * Returns the segment's reader holder, reusing the one already parsed for this segment core when
     * there is one. The caller must {@link #close()} the result, which closes the file only in the
     * fallback case where no core was available to own it.
     *
     * @param leafReader  the leaf reader being searched, whose core owns the reader
     * @param segmentInfo the segment holding the engine file
     * @param fieldName   the sparse field to read
     */
    public static SegmentDiskSeismicSqIndex open(LeafReader leafReader, SegmentInfo segmentInfo, String fieldName) throws IOException {
        final IndexReader.CacheHelper coreCache = leafReader.getCoreCacheHelper();
        if (coreCache == null) {
            // A reader wrapper that exposes no core lifecycle leaves nothing to hang the open file on,
            // so it must not outlive this query.
            return new SegmentDiskSeismicSqIndex(segmentInfo, resolveFileName(segmentInfo, fieldName), false);
        }
        final Key key = new Key(coreCache.getKey(), fieldName);
        SegmentDiskSeismicSqIndex index = OPEN_INDICES.get(key);
        if (index != null) {
            return index;
        }
        // Resolved only on a miss: scanning segmentInfo.files() per query is pure overhead once the
        // reader is parsed, and it is parsed for all but the first query on a core.
        final String fileName = resolveFileName(segmentInfo, fieldName);
        return OPEN_INDICES.computeIfAbsent(key, k -> {
            SegmentDiskSeismicSqIndex created = new SegmentDiskSeismicSqIndex(segmentInfo, fileName, true);
            coreCache.addClosedListener(ignored -> {
                OPEN_INDICES.remove(k);
                created.release();
            });
            return created;
        });
    }

    /** The parsed reader, parsing it on first call. Valid for as long as the caller's reader is open. */
    public synchronized DiskSeismicSqIndexReader reader() throws IOException {
        if (closed) {
            throw new AlreadyClosedException("Sparse index reader has already been closed: " + fileName);
        }
        if (reader == null) {
            reader = DiskSeismicSqIndexReader.open(segmentInfo.dir, fileName);
            log.debug("Parsed sparse index [{}] through an IndexInput, {} bytes of heap", fileName, reader.ramBytesUsed());
        }
        return reader;
    }

    /** Closes the file unless the segment core owns it, in which case closing the core does. */
    @Override
    public void close() {
        if (coreScoped == false) {
            release();
        }
    }

    private synchronized void release() {
        closed = true;
        if (reader != null) {
            IOUtils.closeWhileHandlingException(reader);
            log.debug("Closed sparse index [{}]", fileName);
            reader = null;
        }
    }

    /**
     * The engine file's name in the segment's directory. It is deliberately not folded into the
     * segment's compound file (see
     * {@link org.opensearch.neuralsearch.sparse.codec.SparseCompoundFormat}), so it is a standalone
     * file in {@code segmentInfo.dir} whichever way the segment was written -- and its payload starts
     * at offset 0, which is what lets the alignment padding in the format be computed from the file
     * position.
     */
    private static String resolveFileName(SegmentInfo segmentInfo, String fieldName) throws IOException {
        List<String> engineFiles = CodecUtils.getEngineFiles(SparseEngine.NATIVE.extension(), fieldName, segmentInfo);
        if (engineFiles.isEmpty()) {
            throw new IOException("No native engine file for field [" + fieldName + "] in segment [" + segmentInfo.name + "]");
        }
        return engineFiles.get(0);
    }
}

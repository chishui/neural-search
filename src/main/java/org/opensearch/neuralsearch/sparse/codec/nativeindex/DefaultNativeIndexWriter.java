/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.BinaryVectorUtils;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the native engine file for one sparse field of one segment.
 *
 * The doc values are streamed into off-heap CSR memory, handed to nsparse in a single
 * {@code insertToIndex} call, and the built index is serialized into the segment's
 * {@link SparseEngine#NATIVE} file. Which nsparse index type gets built is derived from the field
 * mapping, so this is also where the mapping's seismic parameters turn into nsparse ones.
 *
 * One instance writes one field; both flush and merge go through {@link #WriteIndex}.
 */
@Log4j2
@AllArgsConstructor
public class DefaultNativeIndexWriter {
    private final SegmentWriteState state;
    private final FieldInfo fieldInfo;

    /**
     * Builds the index over every document the iterator yields and writes it to the segment.
     *
     * A segment in which no document has the field still gets a footer-only file, so the name is
     * always openable. The native index is freed on every path out.
     *
     * @param binaryDocValues the field's sparse vectors, from a flush or a merge
     * @throws IOException if the engine file cannot be written
     */
    public void WriteIndex(BinaryDocValues binaryDocValues) throws IOException {
        int threadCount = SparseSettings.state().getSettingValue(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY);
        final String engineFileName = CodecUtils.buildIndexFileName(
            state.segmentInfo.name,
            SparseEngine.NATIVE.version(),
            fieldInfo.name,
            SparseEngine.NATIVE.extension()
        );
        int totalDoc = state.segmentInfo.maxDoc();
        try (IndexOutput output = state.directory.createOutput(engineFileName, state.context)) {
            WriteBufferResult result = new WriteBufferResult();
            OffHeapSparseVectorsBuffer buffer = writeToBuffer(binaryDocValues, result);
            if (result.getDocIds().isEmpty()) {
                // No document in this segment has the field, so there is no index to
                // build. Still write the footer so the file stays a valid Lucene file.
                CodecUtil.writeFooter(output);
                return;
            }
            int dimension = result.getDimension();
            long indexAddress = NativeLibrary.initIndex(totalDoc, dimension, buildIndexParameters());
            // writeIndex takes ownership of indexAddress and frees it. Until it is
            // reached, nothing else will: an exception from insertToIndex would leak
            // the whole segment's native index, and merges retry on every attempt.
            boolean ownershipTransferred = false;
            try {
                long dataAddresses[] = buffer.getMemoryAddresses();
                NativeLibrary.insertToIndex(
                    indexAddress,
                    Ints.toArray(result.getDocIds()),
                    dataAddresses[0],
                    dataAddresses[1],
                    dataAddresses[2],
                    threadCount
                );
                IndexOutputWrapper indexOutputWrapper = new IndexOutputWrapper(output);
                ownershipTransferred = true;
                NativeLibrary.writeIndex(indexAddress, indexOutputWrapper);
            } finally {
                if (!ownershipTransferred) {
                    NativeLibrary.freeIndex(indexAddress);
                }
            }
            CodecUtil.writeFooter(output);
        } catch (Exception e) {
            log.error("Fails to write native index", e);
            throw e;
        }
    }

    private OffHeapSparseVectorsBuffer writeToBuffer(BinaryDocValues binaryDocValues, WriteBufferResult result) throws IOException {
        ByteSizeValue bytesLimit = SparseSettings.state().getSettingValue(SparseSettings.SPARSE_VECTOR_STREAMING_MEMORY_LIMIT);
        OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(bytesLimit.getBytes());
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            List<Integer> tokens = new ArrayList<>();
            List<Float> weights = new ArrayList<>();
            BinaryVectorUtils.readToList(binaryDocValues.binaryValue(), tokens, weights);
            result.updateMaxTokenId(tokens);
            result.getDocIds().add(docId);
            buffer.addVector(tokens, weights);
            docId = binaryDocValues.nextDoc();
        }
        buffer.flush();
        return buffer;
    }

    /**
     * Translates the field mapping into the nsparse {@code index_factory} parameter map.
     *
     * A field that has not reached the seismic threshold gets a plain inverted index, so a small
     * segment is not clustered; past it, the layout follows the field's {@code forward_index}.
     */
    private Map<String, Object> buildIndexParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        if (PredicateUtils.shouldRunSeisPredicate.test(state.segmentInfo, fieldInfo)) {
            float clusterRatio = SparseFieldUtils.getClusterRatio(fieldInfo);
            int maxDoc = state.segmentInfo.maxDoc();
            int nPostings = SparseFieldUtils.getNPostings(fieldInfo, maxDoc);
            float summaryPruneRatio = SparseFieldUtils.getSummaryPruneRatio(fieldInfo);
            // Both layouts quantize to 8-bit codes over the same range, so a given weight is
            // clamped and rounded identically whichever one the field selects -- and identically
            // to the JVM path. 8-bit codes cut the engine file from 13.9 to 8.0 GiB on base_full,
            // and both are mmap-able, so a large segment's posting lists land in reclaimable page
            // cache rather than on the heap.
            //
            // They differ in where the forward vectors live: seismic_sq keeps one contiguous
            // forward index for the whole field, disk_seismic_sq stores each block's vectors
            // inline next to the block so a query reads only the blocks it selects.
            if (SparseFieldUtils.getSparseForwardIndex(fieldInfo) == SparseForwardIndex.PER_BLOCK) {
                parameters.put("index", "disk_seismic_sq");
            } else {
                parameters.put("index", "seismic_sq");
            }
            parameters.put("quantizer", "8bit");
            parameters.put("vmin", 0.0f);
            parameters.put("vmax", ByteQuantizationUtil.getCeilingValueIngest(fieldInfo));
            parameters.put("lambda", nPostings);
            parameters.put("beta", clusterRatio * nPostings);
            parameters.put("alpha", summaryPruneRatio);
        } else {
            parameters.put("index", "inverted");
        }
        return parameters;
    }

    /**
     * Accumulates metadata detected during {@link #writeToBuffer}, such as
     * doc IDs and the auto-detected dimension (max token ID + 1).
     */
    @Getter
    private static class WriteBufferResult {
        private final List<Integer> docIds = new ArrayList<>();
        private int maxTokenId = 0;

        void updateMaxTokenId(List<Integer> tokens) {
            if (!tokens.isEmpty()) {
                // Doc-value tokens are stored in parser order, not sorted, so the last
                // element is not the largest. Undersizing the dimension here makes
                // nsparse reject the segment with "term_id out of range".
                maxTokenId = Math.max(maxTokenId, Collections.max(tokens));
            }
        }

        int getDimension() {
            return maxTokenId + 1;
        }
    }
}

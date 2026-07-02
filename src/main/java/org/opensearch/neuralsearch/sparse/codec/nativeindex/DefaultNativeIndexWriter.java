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
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.BinaryVectorUtils;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;

import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
@AllArgsConstructor
public class DefaultNativeIndexWriter {
    private final SegmentWriteState state;
    private final FieldInfo fieldInfo;

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
            int dimension = result.getDimension();
            long indexAddress = NativeLibrary.initIndex(totalDoc, dimension, buildIndexParameters());
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
            NativeLibrary.writeIndex(indexAddress, indexOutputWrapper);
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

    private Map<String, Object> buildIndexParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        if (PredicateUtils.shouldRunSeisPredicate.test(state.segmentInfo, fieldInfo)) {
            float ceiling = SparseFieldUtils.getQuantizationCeilingIngest(fieldInfo);
            float clusterRatio = SparseFieldUtils.getClusterRatio(fieldInfo);
            int maxDoc = state.segmentInfo.maxDoc();
            int nPostings = SparseFieldUtils.getNPostings(fieldInfo, maxDoc);
            float summaryPruneRatio = SparseFieldUtils.getSummaryPruneRatio(fieldInfo);
            parameters.put("index", "seismic_sq");
            parameters.put("quantizer", "8bit");
            parameters.put("vmax", ceiling);
            parameters.put("vmin", 0.0f);
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
                maxTokenId = Math.max(maxTokenId, tokens.get(tokens.size() - 1));
            }
        }

        int getDimension() {
            return maxTokenId + 1;
        }
    }
}

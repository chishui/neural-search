/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * DocWeight class to store docID and weight
 */
@AllArgsConstructor
@Getter
@EqualsAndHashCode
public final class DocWeight implements Comparable<DocWeight> {
    private final int docID;
    private final byte weight;

    @Override
    public int compareTo(DocWeight o) {
        return Integer.compare(this.docID, o.docID);
    }

    public int getIntWeight() {
        return ByteQuantizationUtil.getUnsignedByte(weight);
    }

    /**
     * Collapses entries sharing a doc id into one carrying the largest weight, compared unsigned as
     * quantized weights are. This is how a posting folded from several token ids treats a document
     * holding more than one of them: the same rule its folded forward-index vector follows.
     *
     * @param docWeights the entries to collapse, in any order
     * @return one entry per doc id, ordered by doc id
     */
    public static List<DocWeight> mergeByMaxWeight(List<DocWeight> docWeights) {
        List<DocWeight> sorted = new ArrayList<>(docWeights);
        sorted.sort(null);
        List<DocWeight> merged = new ArrayList<>(sorted.size());
        for (DocWeight docWeight : sorted) {
            int last = merged.size() - 1;
            if (last >= 0 && merged.get(last).docID == docWeight.docID) {
                if (ByteQuantizationUtil.compareUnsignedByte(docWeight.weight, merged.get(last).weight) > 0) {
                    merged.set(last, docWeight);
                }
            } else {
                merged.add(docWeight);
            }
        }
        return merged;
    }
}

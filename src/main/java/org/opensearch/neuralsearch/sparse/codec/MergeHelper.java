/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Helper class for managing cache data during segment merges in sparse vector fields.
 */
@NoArgsConstructor
@Log4j2
public class MergeHelper {
    /**
     * Clears cache data for sparse vector fields during segment merge operations.
     *
     * @param mergeStateFacade the merge state containing doc values producers and field info
     * @param fieldInfo specific field to process, or null to process all sparse fields
     * @param consumer callback to handle cache key removal
     * @throws IOException if doc values cannot be accessed
     */
    public void clearCacheData(
        @NonNull MergeStateFacade mergeStateFacade,
        @Nullable FieldInfo fieldInfo,
        @NonNull Consumer<CacheKey> consumer
    ) throws IOException {
        for (DocValuesProducer producer : mergeStateFacade.getDocValuesProducers()) {
            for (FieldInfo field : mergeStateFacade.getMergeFieldInfos()) {
                boolean isNotSparse = !SparseVectorField.isSparseField(field);
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

    /**
     * Retrieves the merged posting list for one folded term across all segments.
     *
     * @param mergeStateFacade merge state containing producers and doc maps
     * @param rawTerms every term folding onto the posting being built, as {@link #getRawTermsByFoldedTerm} groups them
     * @param fieldInfo field information for the sparse vector field
     * @param newIdToFieldProducerIndex array to store field producer index for each new doc ID
     * @param newIdToOldId array to store old doc ID mapping for each new doc ID
     * @return the document weights, one per document: a document under more than one of {@code rawTerms}
     *         keeps its largest weight
     * @throws IOException if postings cannot be accessed
     */
    public List<DocWeight> getMergedPostingForATerm(
        MergeStateFacade mergeStateFacade,
        Collection<BytesRef> rawTerms,
        FieldInfo fieldInfo,
        int[] newIdToFieldProducerIndex,
        int[] newIdToOldId
    ) throws IOException {
        List<DocWeight> docWeights = new ArrayList<>();
        // A document reached twice -- under two of the raw terms, or twice in a posting the reader folded
        // from an older segment's raw terms -- has to collapse to one entry, or it takes two of the
        // n_postings slots. Rare, so the collapse only runs once one is seen.
        FixedBitSet seen = new FixedBitSet(newIdToOldId.length);
        boolean hasDuplicates = false;
        ByteQuantizer byteQuantizer = ByteQuantizationUtil.getByteQuantizerIngest(fieldInfo);
        for (int i = 0; i < mergeStateFacade.getFieldsProducers().length; i++) {
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = mergeStateFacade.getDocValuesProducers()[i].getBinary(fieldInfo);
            if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                continue;
            }
            FieldsProducer fieldsProducer = mergeStateFacade.getFieldsProducers()[i];
            Terms terms = fieldsProducer.terms(fieldInfo.getName());
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            if (termsEnum == null) {
                continue;
            }

            for (BytesRef term : rawTerms) {
                if (!termsEnum.seekExact(term)) {
                    continue;
                }
                PostingsEnum postings = termsEnum.postings(null);
                if (postings == null) {
                    continue;
                }
                boolean isSparsePostings = postings instanceof SparsePostingsEnum;
                int docId = postings.nextDoc();
                for (; docId != PostingsEnum.NO_MORE_DOCS; docId = postings.nextDoc()) {
                    if (docId == -1) {
                        continue;
                    }
                    int newDocId = mergeStateFacade.getDocMaps()[i].get(docId);
                    if (newDocId == -1) {
                        continue;
                    }
                    if (newDocId >= newIdToFieldProducerIndex.length) {
                        throw new IndexOutOfBoundsException("newDocId is larger than array size!");
                    }
                    newIdToFieldProducerIndex[newDocId] = i;
                    newIdToOldId[newDocId] = docId;
                    int freq = postings.freq();
                    byte freqByte = 0;
                    if (isSparsePostings) {
                        // SparsePostingsEnum.freq() already transform byte freq to int
                        freqByte = (byte) freq;
                    } else {
                        // decode to float first
                        freqByte = byteQuantizer.quantize(ValueEncoder.decodeFeatureValue(freq));
                    }
                    hasDuplicates |= seen.getAndSet(newDocId);
                    docWeights.add(new DocWeight(newDocId, freqByte));
                }
            }
        }
        return hasDuplicates ? DocWeight.mergeByMaxWeight(docWeights) : docWeights;
    }

    /**
     * Collects the terms of the segments being merged, grouped under the folded term each one becomes
     * in the merged segment. A source keeps raw terms if it is below the approximate threshold, or a
     * seismic segment written before the clustered postings were folded.
     *
     * @param mergeStateFacade merge state containing field producers
     * @param fieldInfo field information for the sparse vector field
     * @return every folded term, mapped to the source terms that fold onto it (itself included when a
     *         source has it)
     * @throws IOException if terms cannot be accessed
     */
    public Map<BytesRef, Set<BytesRef>> getRawTermsByFoldedTerm(MergeStateFacade mergeStateFacade, FieldInfo fieldInfo) throws IOException {
        Map<BytesRef, Set<BytesRef>> rawTermsByFoldedTerm = new HashMap<>();
        for (int i = 0; i < mergeStateFacade.getFieldsProducers().length; i++) {
            FieldsProducer fieldsProducer = mergeStateFacade.getFieldsProducers()[i];
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = mergeStateFacade.getDocValuesProducers()[i].getBinary(fieldInfo);
            if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                continue;
            }

            Terms terms = fieldsProducer.terms(fieldInfo.getName());
            if (terms instanceof SparseTerms sparseTerms) {
                for (BytesRef term : sparseTerms.getReader().getTerms()) {
                    addRawTerm(rawTermsByFoldedTerm, term);
                }
            } else {
                // fieldsProducer could be a delegate one as we need to merge normal segments into seis segment
                TermsEnum termsEnum = terms.iterator();
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    addRawTerm(rawTermsByFoldedTerm, BytesRef.deepCopyOf(term));
                }
            }
        }
        return rawTermsByFoldedTerm;
    }

    private static void addRawTerm(Map<BytesRef, Set<BytesRef>> rawTermsByFoldedTerm, BytesRef rawTerm) {
        rawTermsByFoldedTerm.computeIfAbsent(SparseVector.prepareTermForShortType(rawTerm), k -> new HashSet<>(2)).add(rawTerm);
    }

    /**
     * A helper function to create merge state facade which enables better testability.
     * @param mergeState {@link MergeState}
     * @return {@link MergeStateFacade}
     */
    public MergeStateFacade convertToMergeStateFacade(MergeState mergeState) {
        return new MergeStateFacade(mergeState);
    }

    /**
     * Create a new SparseDocValuesReader instance
     * @param mergeStateFacade {@link MergeStateFacade}
     * @return {@link SparseDocValuesReader}
     */
    public SparseDocValuesReader newSparseDocValuesReader(MergeStateFacade mergeStateFacade) {
        return new SparseDocValuesReader(mergeStateFacade);
    }
}

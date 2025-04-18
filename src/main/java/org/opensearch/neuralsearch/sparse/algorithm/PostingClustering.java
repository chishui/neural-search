/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.SortedDocInClusterIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class PostingClustering {

    private final static int MINIMAL_DOC_SIZE_OF_CLUSTER = 10;
    private final int lambda;
    private final Clustering clustering;

    public PostingClustering(int lambda, Clustering clustering) {
        this.lambda = lambda;
        this.clustering = clustering;
    }

    private List<DocFreq> preprocess(List<DocFreq> postings) {
        // sort
        List<DocFreq> result = PostingsProcessor.sortByFreq(postings);
        // prune
        result = PostingsProcessor.pruneBySize(result, this.lambda);
        return result;
    }

    public List<DocumentCluster> cluster(List<DocFreq> postings) throws IOException {
        List<DocFreq> preprocessed = preprocess(postings);
        if (preprocessed.isEmpty()) {
            return new ArrayList<>();
        }
        if (preprocessed.size() < MINIMAL_DOC_SIZE_OF_CLUSTER) {
            return Collections.singletonList(
                new DocumentCluster(
                    null,
                    new SortedDocInClusterIterator(preprocessed.stream().map(DocFreq::getDocID).collect(Collectors.toList())),
                    true
                )
            );
        }
        return clustering.cluster(preprocessed);
    }
}

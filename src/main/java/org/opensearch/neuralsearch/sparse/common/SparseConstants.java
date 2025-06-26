/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

public final class SparseConstants {
    public static final String NAME_FIELD = "name";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String N_POSTINGS_FIELD = "n_postings";
    public static final String SUMMARY_PRUNE_RATIO_FIELD = "summary_prune_ratio";
    public static final String SEISMIC = "seismic";
    public static final String CLUSTER_RATIO_FIELD = "cluster_ratio";
    public static final String ALGO_TRIGGER_DOC_COUNT_FIELD = "algo_trigger_doc_count";

    public final class Seismic {
        public static final int DEFAULT_N_POSTINGS = -1; // Determine whether a user has given a specific value
        public static final float DEFAULT_SUMMARY_PRUNE_RATIO = 0.4f;
        public static final float DEFAULT_CLUSTER_RATIO = 0.1f;
        public static final int DEFAULT_ALGO_TRIGGER_DOC_COUNT = 1000000;
        public static final float DEFAULT_POSTING_PRUNE_RATIO = 0.0005f;
        public static final int DEFAULT_POSTING_MINIMUM_LENGTH = 160; // 160 is a good nPostings value when there are 100K documents. We
                                                                      // would only suggest using SEISMIC with over 100K+ documents.
    }
}

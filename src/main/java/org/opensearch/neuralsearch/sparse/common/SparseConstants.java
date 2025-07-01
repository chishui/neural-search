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
        public static final int DEFAULT_N_POSTINGS = -1; // Determine whether a user has given a specific value. If N_POSTINGS is -1, it
                                                         // will disable the N_POSTINGS_FIELD field.
        public static final float DEFAULT_SUMMARY_PRUNE_RATIO = 0.4f; // SUMMARY_PRUNE_RATIO will prune summary into alpha-massed form.
        public static final float DEFAULT_CLUSTER_RATIO = 0.1f; // CLUSTER_RATIO refers to following equation: cluster_num = cluster_ratio *
                                                                // posting_list_length.
        public static final int DEFAULT_ALGO_TRIGGER_DOC_COUNT = 1000000; // We would suggest you using SEISMIC once your dataset size
                                                                          // reaches 1M, where our latency can be 60% of two-phase
                                                                          // algorithm.
        public static final float DEFAULT_POSTING_PRUNE_RATIO = 0.0005f; // The POSTING_PRUNE_RATIO controls how long your posting lists can
                                                                         // keep. This length is nearly linear with the dataset size, which
                                                                         // is an empirical ratio from our tests.
        public static final int DEFAULT_POSTING_MINIMUM_LENGTH = 160; // 160 is a good nPostings value when there are 100K documents. We
                                                                      // would only suggest using SEISMIC with over 100K+ documents.
    }
}

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
    public static final String APPROXIMATE_THRESHOLD_FIELD = "approximate_threshold";

    public static final class Seismic {
        public static final int DEFAULT_N_POSTINGS = 6000;
        public static final float DEFAULT_SUMMARY_PRUNE_RATIO = 0.4f;
        public static final float DEFAULT_CLUSTER_RATIO = 0.1f;
        public static final int DEFAULT_APPROXIMATE_THRESHOLD = 1000000;
    }
}

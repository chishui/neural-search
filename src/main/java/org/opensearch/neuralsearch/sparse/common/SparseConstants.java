/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

public class SparseConstants {
    public static final String NAME_FIELD = "name";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String LAMBDA_FIELD = "n_postings";
    public static final String ALPHA_FIELD = "summary_prune_ratio";
    public static final String SEISMIC = "seismic";
    public static final String CLUSTER_RATIO_FIELD = "cluster_ratio";
    public static final String ALGO_TRIGGER_THRESHOLD_FIELD = "algo_trigger_doc_count";

    public static final int DEFAULT_LAMBDA = 6000;
    public static final float DEFAULT_ALPHA = 0.4f;
    public static final float DEFAULT_CLUSTER_RATIO = 0.1f;
    public static final int ALGO_TRIGGER_DEFAULT_THRESHOLD = 1000000;
}

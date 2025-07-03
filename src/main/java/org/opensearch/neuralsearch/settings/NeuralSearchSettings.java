/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.settings;

import org.opensearch.common.settings.Setting;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.threadpool.FixedExecutorBuilder;

/**
 * Class defines settings specific to neural-search plugin
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NeuralSearchSettings {
    public static final int INITIAL_INDEX_THREAD_QTY = -1; // -1 represents that user did not give a specific
                                                           // thread
                                                           // quantity
    public static int INDEX_THREAD_QTY_MAX = 1024; // Initial max value, will be updated based on actual CPU cores
    public static final String SPARSE_ALGO_PARAM_INDEX_THREAD_QTY = "sparse.algo_param.index_thread_qty";
    public static Integer SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY = INITIAL_INDEX_THREAD_QTY;
    /**
     * Gates the functionality of hybrid search
     * Currently query phase searcher added with hybrid search will conflict with concurrent search in core.
     * Once that problem is resolved this feature flag can be removed.
     */
    public static final Setting<Boolean> NEURAL_SEARCH_HYBRID_SEARCH_DISABLED = Setting.boolSetting(
        "plugins.neural_search.hybrid_search_disabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Limits the number of document fields that can be passed to the reranker.
     */
    public static final Setting<Integer> RERANKER_MAX_DOC_FIELDS = Setting.intSetting(
        "plugins.neural_search.reranker_max_document_fields",
        50,
        Setting.Property.NodeScope
    );

    /**
     * Enables or disables the Stats API and event stat collection.
     * If API is called when stats are disabled, the response will 403.
     * Event stat increment calls are also treated as no-ops.
     */
    public static final Setting<Boolean> NEURAL_STATS_ENABLED = Setting.boolSetting(
        "plugins.neural_search.stats_enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static Setting<Integer> SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
        SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY,
        -1, // -1 means that user did not give specific thread quantity
        INDEX_THREAD_QTY_MAX,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static FixedExecutorBuilder updateThreadQtySettings(Settings settings) {
        int maxThreadQty = OpenSearchExecutors.allocatedProcessors(settings);
        int threadQty = SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        if (threadQty == INITIAL_INDEX_THREAD_QTY) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        }
        INDEX_THREAD_QTY_MAX = maxThreadQty;
        SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY = threadQty;
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
            SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
            SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY,
            1,
            INDEX_THREAD_QTY_MAX,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        return new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            threadQty,
            -1,
            String.format("thread_pool.%s", ClusterTrainingRunning.THREAD_POOL_NAME),
            false
        );
    }

}

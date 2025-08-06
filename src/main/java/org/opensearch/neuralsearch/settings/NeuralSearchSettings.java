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
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Class defines settings specific to neural-search plugin
 * DEFAULT_INDEX_THREAD_QTY: -1 represents that user did not give a specific thread quantity
 * MAX_INDEX_THREAD_QTY: Initial max value, will be updated based on actual CPU cores
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NeuralSearchSettings {

    public static final int DEFAULT_INDEX_THREAD_QTY = -1;
    public static int MAX_INDEX_THREAD_QTY = 1024;
    public static final String SPARSE_ALGO_PARAM_INDEX_THREAD_QTY = "neural.sparse.algo_param.index_thread_qty";
    public static Integer SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY = DEFAULT_INDEX_THREAD_QTY;
    public static final String NEURAL_CIRCUIT_BREAKER_NAME = "neural_search";

    private static final String DEFAULT_CIRCUIT_BREAKER_LIMIT = "50%";
    private static final double DEFAULT_CIRCUIT_BREAKER_OVERHEAD = 1.0d;
    private static final double MINIMUM_CIRCUIT_BREAKER_OVERHEAD = 0.0d;

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
        MAX_INDEX_THREAD_QTY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static int updateThreadQtySettings(Settings settings) {
        int maxThreadQty = OpenSearchExecutors.allocatedProcessors(settings);
        int threadQty = SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        if (threadQty == DEFAULT_INDEX_THREAD_QTY) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        }
        MAX_INDEX_THREAD_QTY = maxThreadQty;
        SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY = threadQty;
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
            SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
            SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY,
            1,
            MAX_INDEX_THREAD_QTY,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );
        return threadQty;
    }

    /**
     * A constant by which the neural memory estimations are multiplied to determine the final estimation. Default is 1.
     */
    public static final Setting<Double> NEURAL_CIRCUIT_BREAKER_OVERHEAD = Setting.doubleSetting(
        "plugins.neural_search.circuit_breaker.overhead",
        DEFAULT_CIRCUIT_BREAKER_OVERHEAD,
        MINIMUM_CIRCUIT_BREAKER_OVERHEAD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The memory limit for neural circuit breaker. Default is 50% of the JVM heap.
     */
    public static final Setting<ByteSizeValue> NEURAL_CIRCUIT_BREAKER_LIMIT = Setting.memorySizeSetting(
        "plugins.neural_search.circuit_breaker.limit",
        DEFAULT_CIRCUIT_BREAKER_LIMIT,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}

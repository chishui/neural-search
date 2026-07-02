/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.cache.NativeCacheManager;

import java.util.List;
import java.util.Objects;

import static org.opensearch.common.settings.Setting.Property.Final;
import static org.opensearch.common.settings.Setting.Property.IndexScope;
import static org.opensearch.common.settings.Setting.Property.UnmodifiableOnRestore;
import static org.opensearch.common.unit.MemorySizeValue.parseBytesSizeValueOrHeapRatio;

/**
 * It holds index settings of a sparse vector index.
 */
public class SparseSettings {
    public static final String SPARSE_INDEX = "index.sparse";
    public static final String SPARSE_VECTOR_STREAMING_MEMORY_LIMIT = "plugins.neural_search.sparse.vector_streaming_memory.limit";
    public static final String SPARSE_ALGO_PARAM_INDEX_THREAD_QTY = "plugins.neural_search.sparse.algo_param.index_thread_qty";
    public static final String SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT = "plugins.neural_search.memory.circuit_breaker.limit";

    public static final int DEFAULT_INDEX_THREAD_QTY = 1; // Choosing 1 as default value to protect safety
    public static final int MINIMUM_INDEX_THREAD_QTY = 1;
    public static final int MAXIMUM_INDEX_THREAD_QTY = 1024;

    private static final String SPARSE_DEFAULT_VECTOR_STREAMING_MEMORY_LIMIT = "1%";
    private static final String SPARSE_DEFAULT_MEMORY_CIRCUIT_BREAKER_LIMIT = "50%";

    private static SparseSettings INSTANCE;
    private ClusterService clusterService;

    public static synchronized SparseSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new SparseSettings();
        }
        return INSTANCE;
    }

    public void initialize(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        registerSettingsCallbacks(clusterService, settings);
    }

    private void registerSettingsCallbacks(ClusterService clusterService, Settings settings) {
        // Initialize with current values since addSettingsUpdateConsumer only fires on updates
        int initialThreadQty = SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(clusterService.getSettings());
        int maxThreadQty = OpenSearchExecutors.allocatedProcessors(settings);
        ClusterTrainingExecutor.updateThreadPoolSize(maxThreadQty, initialThreadQty);

        ByteSizeValue initialMemoryLimit = SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING.get(clusterService.getSettings());
        NativeCacheManager.instance().updateCacheLimit(initialMemoryLimit.getBytes());

        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING, (setting) -> {
                int maxThreads = OpenSearchExecutors.allocatedProcessors(settings);
                ClusterTrainingExecutor.updateThreadPoolSize(maxThreads, setting);
            });
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(SparseSettings.SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING, (setting) -> {
                long bytes = setting.getBytes();
                NativeCacheManager.instance().updateCacheLimit(bytes);
            });
    }

    public static ByteSizeValue getSparseVectorStreamingMemoryLimit() {
        return SparseSettings.state().getSettingValue(SPARSE_VECTOR_STREAMING_MEMORY_LIMIT);
    }

    public <T> T getSettingValue(String key) {
        return (T) clusterService.getClusterSettings().get(getSetting(key));
    }

    public List<Setting<?>> getSettings() {
        return List.of(
            SparseSettings.IS_SPARSE_INDEX_SETTING,
            SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING,
            SparseSettings.SPARSE_VECTOR_STREAMING_MEMORY_LIMIT_PCT_SETTING,
            SparseSettings.SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING
        );
    }

    private Setting<?> getSetting(String key) {
        if (SPARSE_VECTOR_STREAMING_MEMORY_LIMIT.equals(key)) {
            return SPARSE_VECTOR_STREAMING_MEMORY_LIMIT_PCT_SETTING;
        } else if (SPARSE_ALGO_PARAM_INDEX_THREAD_QTY.equals(key)) {
            return SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING;
        } else if (SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT.equals(key)) {
            return SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING;
        }

        throw new IllegalArgumentException("Cannot find setting by key [" + key + "]");
    }

    /**
     * This setting identifies sparse index.
     */
    public static final Setting<Boolean> IS_SPARSE_INDEX_SETTING = Setting.boolSetting(
        SPARSE_INDEX,
        false,
        IndexScope,
        Final,
        UnmodifiableOnRestore
    );

    public static Setting<Integer> SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING = Setting.intSetting(
        SPARSE_ALGO_PARAM_INDEX_THREAD_QTY,
        DEFAULT_INDEX_THREAD_QTY,
        MINIMUM_INDEX_THREAD_QTY,
        MAXIMUM_INDEX_THREAD_QTY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // This setting controls how much memory should be used to transfer vectors from Java to JNI Layer. The default
    // 1% of the JVM heap
    public static final Setting<ByteSizeValue> SPARSE_VECTOR_STREAMING_MEMORY_LIMIT_PCT_SETTING = Setting.memorySizeSetting(
        SPARSE_VECTOR_STREAMING_MEMORY_LIMIT,
        SPARSE_DEFAULT_VECTOR_STREAMING_MEMORY_LIMIT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final OsProbe osProbe = OsProbe.getInstance();

    /**
     * Parses the sparse memory circuit breaker value. When the value ends with "%", it is interpreted as a
     * percentage of the eligible native memory (physical memory minus JVM heap). Otherwise, it is parsed as
     * an absolute byte size value.
     *
     * @param sValue       the setting value string (e.g. "50%" or "4gb")
     * @param defaultValue the default value to use when sValue is null
     * @param settingName  the name of the setting (used in error messages)
     * @return the resolved ByteSizeValue
     */
    public static ByteSizeValue parseSparseMemoryCircuitBreakerValue(String sValue, ByteSizeValue defaultValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue != null && sValue.endsWith("%")) {
            final String percentAsString = sValue.substring(0, sValue.length() - 1);
            try {
                final double percent = Double.parseDouble(percentAsString);
                if (percent < 0 || percent > 100) {
                    throw new OpenSearchParseException("percentage should be in [0-100], got [{}]", percentAsString);
                }
                long physicalMemoryInBytes = osProbe.getTotalPhysicalMemorySize();
                if (physicalMemoryInBytes <= 0) {
                    throw new IllegalStateException("Physical memory size could not be determined");
                }
                long esJvmSizeInBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();
                long eligibleMemoryInBytes = physicalMemoryInBytes - esJvmSizeInBytes;
                return new ByteSizeValue((long) ((percent / 100) * eligibleMemoryInBytes));
            } catch (NumberFormatException e) {
                throw new OpenSearchParseException("failed to parse [{}] as a double", e, percentAsString);
            }
        } else {
            return parseBytesSizeValueOrHeapRatio(sValue, settingName);
        }
    }

    // This setting controls the circuit breaker limit for native memory used by sparse indices.
    // The default is 50% of eligible native memory (physical memory minus JVM heap).
    public static final Setting<ByteSizeValue> SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT_SETTING = new Setting<>(
        SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT,
        SPARSE_DEFAULT_MEMORY_CIRCUIT_BREAKER_LIMIT,
        (s) -> parseSparseMemoryCircuitBreakerValue(s, ByteSizeValue.ZERO, SPARSE_MEMORY_CIRCUIT_BREAKER_LIMIT),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
}

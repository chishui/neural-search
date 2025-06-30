/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class ClusterTrainingRunning {
    private static ThreadPool threadpool = null;
    private static ClusterTrainingRunning INSTANCE;
    public static final String THREAD_POOL_NAME = "cluster_training_thread_pool";
    private static ClusterService clusterService = null;

    public static void initialize(ThreadPool threadPool, ClusterService clusterService) {
        ClusterTrainingRunning.threadpool = threadPool;
        ClusterTrainingRunning.clusterService = clusterService;
    }

    public static synchronized ClusterTrainingRunning getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ClusterTrainingRunning();
        }
        return INSTANCE;
    }

    public Executor getExecutor() {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME);
    }

    public void run(Runnable runnable) {
        ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).execute(runnable);
    }

    public <T> Future<T> submit(Callable<T> callable) {
        return ClusterTrainingRunning.threadpool.executor(THREAD_POOL_NAME).submit(callable);
    }

    public int getIndexThreadCount() {
        if (clusterService == null) {
            return NeuralSearchSettings.SPARSE_DEFAULT_ALGO_PARAM_INDEX_THREAD_QTY;
        }

        // Read from cluster settings for dynamic settings
        return clusterService.getClusterSettings().get(NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING);
    }

    public Semaphore getThreadLimiter() {
        int maxThreads = getIndexThreadCount();
        return new Semaphore(maxThreads);
    }
}

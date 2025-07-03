/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.Node;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClusterTrainingRunningTests extends OpenSearchTestCase {

    public void testThreadPoolInitialization() {
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test-node").build();

        int threadQty;
        int maxThreadQty = 8; // Mock available processors
        if (NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings) == -1) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        } else {
            threadQty = NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        }

        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            threadQty,
            -1,
            String.format("thread_pool.%s", ClusterTrainingRunning.THREAD_POOL_NAME),
            false
        );

        ThreadPool threadPool = new ThreadPool(settings, builder);

        try {
            ExecutorService executor = threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME);
            assertNotNull("Thread pool should be initialized", executor);
            assertFalse("Thread pool should not be shutdown", executor.isShutdown());
        } finally {
            threadPool.shutdown();
        }
    }

    public void testExecutorRetrieval() {
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test-node").build();

        int threadQty;
        int maxThreadQty = 8; // Mock available processors
        if (NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings) == -1) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        } else {
            threadQty = NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        }

        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            threadQty,
            -1,
            String.format("thread_pool.%s", ClusterTrainingRunning.THREAD_POOL_NAME),
            false
        );

        ThreadPool threadPool = new ThreadPool(settings, builder);

        try {
            ExecutorService executor = threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME);
            assertNotNull("Should retrieve executor successfully", executor);
            assertSame("Should return same executor instance", executor, threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testTaskSubmission() throws InterruptedException {
        Settings settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test-node").build();

        int threadQty;
        int maxThreadQty = 8; // Mock available processors
        if (NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings) == -1) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        } else {
            threadQty = NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        }

        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            threadQty,
            -1,
            String.format("thread_pool.%s", ClusterTrainingRunning.THREAD_POOL_NAME),
            false
        );

        ThreadPool threadPool = new ThreadPool(settings, builder);

        try {
            ExecutorService executor = threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME);
            CountDownLatch latch = new CountDownLatch(1);

            executor.submit(() -> { latch.countDown(); });

            assertTrue("Task should execute successfully", latch.await(5, TimeUnit.SECONDS));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testCustomThreadQuantity() {
        Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "test-node")
            .put(NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY, 6)
            .build();

        int threadQty;
        int maxThreadQty = 8; // Mock available processors
        if (NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings) == -1) {
            threadQty = Math.max(maxThreadQty / 2, 1);
        } else {
            threadQty = NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY_SETTING.get(settings);
        }

        assertEquals("Custom thread quantity should be used", 6, threadQty);

        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            threadQty,
            -1,
            String.format("thread_pool.%s", ClusterTrainingRunning.THREAD_POOL_NAME),
            false
        );

        ThreadPool threadPool = new ThreadPool(settings, builder);

        try {
            ExecutorService executor = threadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME);
            assertNotNull("Thread pool should be initialized with custom thread quantity", executor);
        } finally {
            threadPool.shutdown();
        }
    }
}

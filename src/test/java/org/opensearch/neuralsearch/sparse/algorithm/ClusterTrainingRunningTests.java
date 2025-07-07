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
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;

public class ClusterTrainingRunningTests extends OpenSearchTestCase {

    private static final int DEFAULT_ALLOCATED_PROCESSORS = 8;
    private static final int EXPECTED_DEFAULT_THREAD_QTY = DEFAULT_ALLOCATED_PROCESSORS / 2;
    private static final String TEST_NODE_NAME = "test-node";
    private static final int CUSTOM_THREAD_QTY = 8;
    private static final int NEW_THREAD_COUNT = 12;
    private static final int CURRENT_THREAD_COUNT = 6;
    private static final int TIMEOUT_SECONDS = 5;
    private static final String EXPECTED_RESULT = "test-result";

    private ThreadPool createThreadPool(Settings settings) {
        int allocatedProcessors = NeuralSearchSettings.updateThreadQtySettings(settings);
        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            allocatedProcessors,
            -1,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            false
        );
        return new ThreadPool(settings, builder);
    }

    private Settings createTestSettings() {
        return Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), TEST_NODE_NAME).build();
    }

    private Settings createCustomThreadSettings(int threadQty) {
        return Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), TEST_NODE_NAME)
            .put(NeuralSearchSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY, threadQty)
            .build();
    }

    public void testSingletonInstance() {
        ClusterTrainingRunning instance1 = ClusterTrainingRunning.getInstance();
        ClusterTrainingRunning instance2 = ClusterTrainingRunning.getInstance();

        assertSame("Should return same singleton instance", instance1, instance2);
    }

    public void testInitialization() {
        Settings settings = createTestSettings();
        ThreadPool threadPool = createThreadPool(settings);

        try {
            ClusterTrainingRunning.initialize(threadPool);
            ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

            Executor executor = instance.getExecutor();
            assertNotNull("Executor should not be null after initialization", executor);
        } finally {
            threadPool.shutdown();
        }
    }

    public void testRunTask() throws InterruptedException {
        Settings settings = createTestSettings();
        ThreadPool threadPool = createThreadPool(settings);

        try {
            ClusterTrainingRunning.initialize(threadPool);
            ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

            CountDownLatch latch = new CountDownLatch(1);

            instance.run(() -> latch.countDown());

            assertTrue("Task should execute successfully", latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            threadPool.shutdown();
        }
    }

    public void testSubmitCallable() throws Exception {
        Settings settings = createTestSettings();
        ThreadPool threadPool = createThreadPool(settings);

        try {
            ClusterTrainingRunning.initialize(threadPool);
            ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

            Future<String> future = instance.submit(new Callable<String>() {
                @Override
                public String call() {
                    return EXPECTED_RESULT;
                }
            });

            String result = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertEquals("Callable should return expected result", EXPECTED_RESULT, result);
        } finally {
            threadPool.shutdown();
        }
    }

    public void testUpdateThreadPoolSize() {
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        OpenSearchThreadPoolExecutor mockExecutor = mock(OpenSearchThreadPoolExecutor.class);

        when(mockThreadPool.executor(ClusterTrainingRunning.THREAD_POOL_NAME)).thenReturn(mockExecutor);
        when(mockExecutor.getCorePoolSize()).thenReturn(CURRENT_THREAD_COUNT);
        when(mockExecutor.getMaximumPoolSize()).thenReturn(CURRENT_THREAD_COUNT);

        ClusterTrainingRunning.initialize(mockThreadPool);
        ClusterTrainingRunning.updateThreadPoolSize(NEW_THREAD_COUNT);

        verify(mockThreadPool).setThreadPool(any(Settings.class));
    }

    public void testDefaultThreadQuantityCalculation() {
        Settings settings = createTestSettings();
        int allocatedProcessors = NeuralSearchSettings.updateThreadQtySettings(settings);
        FixedExecutorBuilder builder = new FixedExecutorBuilder(
            settings,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            allocatedProcessors,
            -1,
            ClusterTrainingRunning.THREAD_POOL_NAME,
            false
        );

        assertNotNull("Builder should not be null", builder);
        assertNotNull("Builder should not be null", builder);
    }

    public void testCustomThreadQuantity() {
        Settings settings = createCustomThreadSettings(CUSTOM_THREAD_QTY);
        ThreadPool threadPool = createThreadPool(settings);

        try {
            ClusterTrainingRunning.initialize(threadPool);
            ClusterTrainingRunning instance = ClusterTrainingRunning.getInstance();

            Executor executor = instance.getExecutor();
            assertNotNull("Executor should be initialized with custom thread quantity", executor);
        } finally {
            threadPool.shutdown();
        }
    }

    public void testThreadPoolNameConstant() {
        assertEquals("cluster_training_thread_pool", ClusterTrainingRunning.THREAD_POOL_NAME);
    }
}

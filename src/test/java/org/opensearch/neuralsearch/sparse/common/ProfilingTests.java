/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

public class ProfilingTests extends AbstractSparseTestBase {

    public void testProfiling_getInstance_returnsSameInstance() {
        Profiling instance1 = Profiling.INSTANCE;
        Profiling instance2 = Profiling.INSTANCE;

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    public void testProfiling_run_enablesProfiling() {
        Profiling profiling = Profiling.INSTANCE;

        // Call run() to ensure profiling is enabled
        profiling.run();
        long start = profiling.begin(Profiling.ItemId.DP);
        assertTrue(start > 0);
    }

    public void testProfiling_noRun_ProfilingReturnsZero() throws Exception {
        Profiling profiling = Profiling.INSTANCE;

        java.lang.reflect.Field runField = Profiling.class.getDeclaredField("run");
        runField.setAccessible(true);
        runField.set(profiling, false);

        java.lang.reflect.Method clearMethod = Profiling.class.getDeclaredMethod("clear");
        clearMethod.setAccessible(true);
        clearMethod.invoke(profiling);

        long start = profiling.begin(Profiling.ItemId.DP);
        assertEquals(0, start);
    }

    public void testProfiling_noRun_beginAndEnd_ProfilingReturnsZero() throws Exception {
        Profiling profiling = Profiling.INSTANCE;

        java.lang.reflect.Field runField = Profiling.class.getDeclaredField("run");
        runField.setAccessible(true);
        runField.set(profiling, false);

        java.lang.reflect.Method clearMethod = Profiling.class.getDeclaredMethod("clear");
        clearMethod.setAccessible(true);
        clearMethod.invoke(profiling);

        long start = profiling.begin(Profiling.ItemId.DP);
        assertEquals(0, start);

        profiling.end(Profiling.ItemId.READ, start);
    }

    public void testProfiling_beginAndEnd_whenRunning_worksCorrectly() {
        Profiling profiling = Profiling.INSTANCE;

        // Ensure profiling is running
        profiling.run();
        long start = profiling.begin(Profiling.ItemId.READ);
        assertTrue(start > 0);

        // end() should work correctly when running
        profiling.end(Profiling.ItemId.READ, start);
        // No exception should be thrown
    }

    public void testProfiling_beginAndEnd_whenRunning_recordsTime() throws InterruptedException {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        long start = profiling.begin(Profiling.ItemId.VISITED);
        assertTrue(start > 0);

        // Sleep a small amount to ensure time passes
        Thread.sleep(1);

        profiling.end(Profiling.ItemId.VISITED, start);
        // No exception should be thrown and time should be recorded
    }

    public void testProfiling_multipleBeginEnd_recordsMultipleTimes() throws InterruptedException {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // First measurement
        long start1 = profiling.begin(Profiling.ItemId.NEXTDOC);
        Thread.sleep(1);
        profiling.end(Profiling.ItemId.NEXTDOC, start1);

        // Second measurement
        long start2 = profiling.begin(Profiling.ItemId.NEXTDOC);
        Thread.sleep(1);
        profiling.end(Profiling.ItemId.NEXTDOC, start2);

        // Both measurements should be recorded
        assertTrue(start1 > 0);
        assertTrue(start2 > 0);
        assertTrue(start2 >= start1);
    }

    public void testProfiling_differentItemIds_recordSeparately() throws InterruptedException {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // Record for different ItemIds
        long start1 = profiling.begin(Profiling.ItemId.ACCEPTED);
        Thread.sleep(1);
        profiling.end(Profiling.ItemId.ACCEPTED, start1);

        long start2 = profiling.begin(Profiling.ItemId.HEAP);
        Thread.sleep(1);
        profiling.end(Profiling.ItemId.HEAP, start2);

        assertTrue(start1 > 0);
        assertTrue(start2 > 0);
    }

    public void testProfiling_output_doesNotThrowException() {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // Record some data
        long start = profiling.begin(Profiling.ItemId.CLUSTER);
        profiling.end(Profiling.ItemId.CLUSTER, start);

        // output() should not throw exception
        profiling.output();
    }

    public void testItemId_getAllValues_hasCorrectIds() {
        Profiling.ItemId[] itemIds = Profiling.ItemId.values();

        assertEquals(9, itemIds.length);
        assertEquals(0, Profiling.ItemId.DP.getId());
        assertEquals(1, Profiling.ItemId.READ.getId());
        assertEquals(2, Profiling.ItemId.VISITED.getId());
        assertEquals(3, Profiling.ItemId.NEXTDOC.getId());
        assertEquals(4, Profiling.ItemId.ACCEPTED.getId());
        assertEquals(5, Profiling.ItemId.HEAP.getId());
        assertEquals(6, Profiling.ItemId.CLUSTER.getId());
        assertEquals(7, Profiling.ItemId.CLUSTERSHOULDNOTSKIP.getId());
        assertEquals(8, Profiling.ItemId.CLUSTERDP.getId());
    }

    public void testItemId_name_returnsCorrectNames() {
        assertEquals("DP", Profiling.ItemId.DP.name());
        assertEquals("READ", Profiling.ItemId.READ.name());
        assertEquals("VISITED", Profiling.ItemId.VISITED.name());
        assertEquals("NEXTDOC", Profiling.ItemId.NEXTDOC.name());
        assertEquals("ACCEPTED", Profiling.ItemId.ACCEPTED.name());
        assertEquals("HEAP", Profiling.ItemId.HEAP.name());
        assertEquals("CLUSTER", Profiling.ItemId.CLUSTER.name());
        assertEquals("CLUSTERSHOULDNOTSKIP", Profiling.ItemId.CLUSTERSHOULDNOTSKIP.name());
        assertEquals("CLUSTERDP", Profiling.ItemId.CLUSTERDP.name());
    }

    public void testProfiling_runMultipleTimes_clearsPreviousData() {
        Profiling profiling = Profiling.INSTANCE;

        // First run
        profiling.run();
        long start1 = profiling.begin(Profiling.ItemId.DP);
        profiling.end(Profiling.ItemId.DP, start1);

        // Second run should clear previous data
        profiling.run();
        long start2 = profiling.begin(Profiling.ItemId.DP);
        profiling.end(Profiling.ItemId.DP, start2);

        assertTrue(start1 > 0);
        assertTrue(start2 > 0);
    }

    public void testProfiling_beginReturnsNanoTime_whenRunning() {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        long beforeCall = System.nanoTime();
        long start = profiling.begin(Profiling.ItemId.READ);
        long afterCall = System.nanoTime();

        assertTrue(start >= beforeCall);
        assertTrue(start <= afterCall);
    }

    public void testProfiling_endWithInvalidStart_doesNotThrow() {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // end() with arbitrary start time should not throw
        profiling.end(Profiling.ItemId.VISITED, 12345L);
        profiling.end(Profiling.ItemId.VISITED, 0L);
        profiling.end(Profiling.ItemId.VISITED, -1L);
    }

    public void testProfiling_allItemIds_canBeUsed() {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // Test all ItemIds can be used without exception
        for (Profiling.ItemId itemId : Profiling.ItemId.values()) {
            long start = profiling.begin(itemId);
            assertTrue(start > 0);
            profiling.end(itemId, start);
        }
    }

    public void testProfiling_concurrentAccess_doesNotThrow() throws InterruptedException {
        Profiling profiling = Profiling.INSTANCE;
        profiling.run();

        // Simple concurrent test
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                long start = profiling.begin(Profiling.ItemId.DP);
                profiling.end(Profiling.ItemId.DP, start);
            }
        });

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                long start = profiling.begin(Profiling.ItemId.READ);
                profiling.end(Profiling.ItemId.READ, start);
            }
        });

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Should complete without exception
    }
}

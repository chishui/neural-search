/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.index.SegmentInfo;
import org.junit.After;
import org.junit.Before;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorWriter;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;

public class ForwardIndexCacheItemTests extends AbstractSparseTestBase {

    private static final int testDocCount = 10;
    private static final String testFieldName = "test_field";

    private CacheKey cacheKey;
    private SegmentInfo segmentInfo;

    /**
     * Set up the test environment before each test.
     * Creates a segment info and cache key for testing.
     */
    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();

        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        cacheKey = new CacheKey(segmentInfo, testFieldName);
    }

    /**
     * Tear down the test environment after each test.
     * Removes the test index from the cache.
     */
    @After
    @Override
    public void tearDown() throws Exception {
        ForwardIndexCache.getInstance().removeIndex(cacheKey);
        super.tearDown();
    }

    /**
     * Tests that getOrCreate returns the same instance when called with an existing key.
     * This verifies the caching behavior of the ForwardIndexCache.
     */
    public void test_getOrCreate_withExistingKey() {
        ForwardIndexCacheItem createdIndex = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        ForwardIndexCacheItem retrievedIndex = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        assertSame("Should return the same index instance", createdIndex, retrievedIndex);
    }

    /**
     * Tests that getOrCreate throws NullPointerException when called with a null key.
     * This verifies the @NonNull annotation on the key parameter.
     */
    public void test_getOrCreate_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            ForwardIndexCache.getInstance().getOrCreate(null, testDocCount);
        });

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that get returns the same instance that was created with getOrCreate.
     * This verifies the retrieval functionality of the ForwardIndexCache.
     */
    public void tes_get_withExistingKey() {
        ForwardIndexCacheItem createdIndex = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        ForwardIndexCacheItem retrievedIndex = ForwardIndexCache.getInstance().get(cacheKey);

        assertSame("Should return the same index instance", createdIndex, retrievedIndex);
    }

    /**
     * Tests that get returns null when called with a key that doesn't exist in the cache.
     * This verifies the behavior for non-existent keys.
     */
    public void test_get_withNonExistingKey() {
        ForwardIndexCacheItem index = ForwardIndexCache.getInstance().get(cacheKey);

        assertNull("Index should be null for non-existent key", index);
    }

    /**
     * Tests that get throws NullPointerException when called with a null key.
     * This verifies the @NonNull annotation on the key parameter.
     */
    public void test_get_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { ForwardIndexCache.getInstance().get(null); });

        assertEquals("key is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests that removeIndex correctly removes an item from the cache.
     * This verifies the removal functionality of the ForwardIndexCache.
     */
    public void test_removeIndex() {
        ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);
        assertNotNull("Index should exist", ForwardIndexCache.getInstance().get(cacheKey));

        ForwardIndexCache.getInstance().removeIndex(cacheKey);
        assertNull("Index should be removed", ForwardIndexCache.getInstance().get(cacheKey));
    }

    /**
     * Tests that reading a vector with an out-of-bounds index returns null.
     * This verifies the bounds checking in the SparseVectorReader.
     */
    @SneakyThrows
    public void test_readerRead_withOutOfBoundVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVector readVector = reader.read(testDocCount + 11);

        assertNull("Read out of bound vector should return null", readVector);
    }

    /**
     * Tests that a vector can be successfully inserted and read back.
     * This verifies the basic functionality of the writer and reader.
     */
    @SneakyThrows
    public void test_writerInsert_withValidVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertEquals("Read vector should match inserted vector", vector, readVector);
    }

    /**
     * Tests that inserting a vector with an out-of-bounds index is ignored.
     * This verifies the bounds checking in the SparseVectorWriter.
     */
    @SneakyThrows
    public void test_writerInsert_withOutOfBoundVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        long ramBytesUsed1 = cacheItem.ramBytesUsed();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(testDocCount + 1, vector1);
        long ramBytesUsed2 = cacheItem.ramBytesUsed();
        assertEquals("Inserting vector out of bound will not increase memory usage", ramBytesUsed1, ramBytesUsed2);
    }

    /**
     * Tests that inserting a vector at an index that already has a vector is ignored.
     * This verifies that the writer doesn't overwrite existing vectors.
     */
    @SneakyThrows
    public void test_writerInsert_withNullVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        // Test inserting null vector should be ignored and does not throw exception
        writer.insert(2, null);
        assertNull("Vector should still be null", reader.read(2));
    }

    /**
     * Tests that inserting a vector at an index that already has a vector is ignored.
     * This verifies that the writer doesn't overwrite existing vectors.
     */
    @SneakyThrows
    public void test_writerInsert_skipsDuplicates() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(0, vector1);
        assertEquals("First vector should be inserted", vector1, reader.read(0));

        SparseVector vector2 = createVector(5, 6, 7, 8);
        writer.insert(0, vector2);
        assertEquals("Original vector should remain unchanged", vector1, reader.read(0));
    }

    /**
     * Tests that vector insertion fails gracefully when the circuit breaker throws an exception.
     * This verifies the error handling in the SparseVectorWriter.
     */
    @SneakyThrows
    public void test_writerInsert_whenCircuitBreakerThrowException() {
        doThrow(new CircuitBreakingException("Memory limit exceeded", CircuitBreaker.Durability.PERMANENT)).when(mockedCircuitBreaker)
            .addEstimateBytesAndMaybeBreak(anyLong(), anyString());

        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        SparseVectorReader reader = cacheItem.getReader();
        SparseVectorWriter writer = cacheItem.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < testDocCount; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        SparseVector vector = createVector(1, 2, 3, 4);
        writer.insert(0, vector);

        SparseVector readVector = reader.read(0);
        assertNull("Read vector should be null", readVector);
    }

    /**
     * Tests that ramBytesUsed correctly reports the memory usage after vectors are inserted.
     * This verifies the memory tracking functionality of the ForwardIndexCacheItem.
     */
    @SneakyThrows
    public void test_ramBytesUsed_withInsertedVector() {
        ForwardIndexCacheItem cacheItem = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        long initialRam = cacheItem.ramBytesUsed();
        SparseVectorWriter writer = cacheItem.getWriter();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        SparseVector vector2 = createVector(5, 6, 7, 8, 9, 10);
        writer.insert(0, vector1);
        writer.insert(1, vector2);
        long ramWithVectors = cacheItem.ramBytesUsed();

        assertEquals(
            "RAM usage should increase by the size of inserted vectors",
            ramWithVectors - initialRam,
            vector1.ramBytesUsed() + vector2.ramBytesUsed()
        );
    }

    /**
     * Tests that creating multiple indices with different keys returns different instances.
     * This verifies that the ForwardIndexCache correctly differentiates between different keys.
     */
    public void test_create_withMultipleIndices() {
        ForwardIndexCacheItem cacheItem1 = ForwardIndexCache.getInstance().getOrCreate(cacheKey, testDocCount);

        CacheKey cacheKey2 = new CacheKey(segmentInfo, "another_field");
        ForwardIndexCacheItem cacheItem2 = ForwardIndexCache.getInstance().getOrCreate(cacheKey2, testDocCount);

        assertNotSame("Should be different index instances", cacheItem1, cacheItem2);
    }
}

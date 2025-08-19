/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.NonNull;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class AbstractLRUCacheTests extends AbstractSparseTestBase {

    /**
     * Test that updateAccess correctly updates the access order in the LRU cache
     */
    public void test_updateAccess_returnsLeastRecentlyUsedItem() {
        TestLRUCache testCache = new TestLRUCache();

        String key1 = "key1";
        String key2 = "key2";

        // Add keys to the cache
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Verify key1 is the least recently used
        assertEquals(key1, testCache.getLeastRecentlyUsedItem());

        // Access key1 again to make it most recently used
        testCache.updateAccess(key1);

        // Now key2 should be the least recently used
        assertEquals(key2, testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that updateAccess handles null keys gracefully
     */
    public void test_updateAccess_withNullKey() {
        TestLRUCache testCache = new TestLRUCache();

        // Add a key to the cache
        testCache.updateAccess("key1");

        // Try to update with null key
        testCache.updateAccess(null);

        // Verify the cache still has the original key
        assertEquals(1, testCache.accessRecencySet.size());
        assertEquals("key1", testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that getLeastRecentlyUsedItem returns null when the cache is empty
     */
    public void test_getLeastRecentlyUsedItem_returnsNullWithEmptyCache() {
        TestLRUCache testCache = new TestLRUCache();

        assertNull(testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that getLeastRecentlyUsedItem returns the correct item
     */
    public void test_getLeastRecentlyUsedItem() {
        TestLRUCache testCache = new TestLRUCache();

        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";

        // Add keys in order
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);
        testCache.updateAccess(key3);

        // Verify key1 is the least recently used
        assertEquals(key1, testCache.getLeastRecentlyUsedItem());

        // Access key1 and key2 to make key3 the least recently used
        testCache.updateAccess(key1);
        testCache.updateAccess(key2);

        // Now key3 should be the least recently used
        assertEquals(key3, testCache.getLeastRecentlyUsedItem());
    }

    /**
     * Test that evict does nothing when ramBytesToRelease is zero or negative
     */
    public void test_evict_withZeroAndNegativeBytes() {
        TestLRUCache testCache = spy(new TestLRUCache());

        // Add some items to the cache
        testCache.updateAccess("key1");
        testCache.updateAccess("key2");

        // Try to evict with zero bytes
        testCache.evict(0);
        assertEquals(2, testCache.accessRecencySet.size());

        // Try to evict with negative bytes
        testCache.evict(-10);
        assertEquals(2, testCache.accessRecencySet.size());

        // Verify doEviction was never called
        verify(testCache, never()).doEviction(anyString());
    }

    /**
     * Test that evict correctly evicts items until enough memory is freed
     */
    public void test_evict_untilEnoughMemoryFreed() {
        TestLRUCache testCache = spy(new TestLRUCache());

        // Set up the test cache to return specific byte values for eviction
        testCache.bytesFreedPerEviction = 50;

        // Add items to the cache
        testCache.updateAccess("key1");
        testCache.updateAccess("key2");
        testCache.updateAccess("key3");

        // Evict 120 bytes (should evict 3 items)
        testCache.evict(120);

        // Verify the cache is now empty
        assertTrue(testCache.accessRecencySet.isEmpty());

        // Verify doEviction was called 3 times
        verify(testCache, times(1)).doEviction("key1");
        verify(testCache, times(1)).doEviction("key2");
        verify(testCache, times(1)).doEviction("key3");
    }

    /**
     * Test that evict stops when the cache becomes empty
     */
    public void test_evict_stopsWhenCacheEmpty() {
        TestLRUCache testCache = spy(new TestLRUCache());

        // Set up the test cache to return specific byte values for eviction
        testCache.bytesFreedPerEviction = 0;

        // Add one item to the cache
        testCache.updateAccess("key1");

        // Try to evict 100 bytes (more than available)
        testCache.evict(100);

        // Verify the cache is now empty
        assertTrue(testCache.accessRecencySet.isEmpty());

        // Verify doEviction was called only once
        verify(testCache, times(1)).doEviction("key1");
    }

    /**
     * Test that evictItem correctly removes an item from the access map
     */
    public void test_evictItem() {
        TestLRUCache testCache = spy(new TestLRUCache());

        // Add items to the cache
        testCache.updateAccess("key1");
        testCache.updateAccess("key2");

        // Evict key1
        long bytesFreed = testCache.evictItem("key1");

        // Verify key1 was removed from the access map
        assertFalse(testCache.accessRecencySet.contains("key1"));
        assertTrue(testCache.accessRecencySet.contains("key2"));

        // Verify the correct number of bytes was returned
        assertEquals(testCache.bytesFreedPerEviction, bytesFreed);
    }

    /**
     * Test that evictItem returns 0 when the key doesn't exist
     */
    public void test_evictItem_withNonExistentKey() {
        TestLRUCache testCache = spy(new TestLRUCache());

        // Add an item to the cache
        testCache.updateAccess("key1");

        // Try to evict a non-existent key
        long bytesFreed = testCache.evictItem("nonexistent");

        // Verify no bytes were freed
        assertEquals(0, bytesFreed);

        // Verify doEviction was not called
        verify(testCache, never()).doEviction("nonexistent");
    }

    /**
     * Test that removeIndex calls the abstract method with the correct key
     */
    public void test_removeIndex() {
        TestLRUCache testCache = spy(new TestLRUCache());

        CacheKey cacheKey = mock(CacheKey.class);

        // Call removeIndex
        testCache.removeIndex(cacheKey);

        // Verify the abstract method was called with the correct key
        verify(testCache, times(1)).doRemoveIndex(cacheKey);
    }

    /**
     * Test that removeIndex throws NullPointerException when key is null
     */
    public void test_removeIndex_withNullKey() {
        TestLRUCache testCache = spy(new TestLRUCache());
        NullPointerException exception = expectThrows(NullPointerException.class, () -> testCache.removeIndex(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    /**
     * A concrete implementation of AbstractLRUCache for testing
     */
    private static class TestLRUCache extends AbstractLRUCache<String> {

        long bytesFreedPerEviction = 0;

        @Override
        protected long doEviction(String s) {
            return bytesFreedPerEviction;
        }

        @Override
        protected void logEviction(String key, long bytesFreed) {
            // Do nothing for testing
        }

        @Override
        public void removeIndex(@NonNull CacheKey cacheKey) {
            doRemoveIndex(cacheKey);
        }

        public void doRemoveIndex(@NonNull CacheKey cacheKey) {
            // Do nothing for testing
        }
    }
}

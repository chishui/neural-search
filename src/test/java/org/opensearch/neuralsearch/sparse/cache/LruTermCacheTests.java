/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.SneakyThrows;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.util.List;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LruTermCacheTests extends AbstractSparseTestBase {

    private BytesRef term1;
    private BytesRef term2;
    private BytesRef term3;
    private CacheKey cacheKey1;
    private CacheKey cacheKey2;
    private TestLruTermCache testCache;

    @Before
    public void setUp() {
        super.setUp();

        cacheKey1 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "lru_term_cache_1");
        cacheKey2 = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "lru_term_cache_2");

        term1 = new BytesRef("term1");
        term2 = new BytesRef("term2");
        term3 = new BytesRef("term3");

        ClusteredPostingCache.getInstance().getOrCreate(cacheKey1);

        testCache = new TestLruTermCache();
        testCache.clearAll();
    }

    /**
     * Test that getInstance returns the singleton instance
     */
    public void test_getInstance_returnsSingletonInstance() {
        LruTermCache instance1 = LruTermCache.getInstance();
        LruTermCache instance2 = LruTermCache.getInstance();

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }

    /**
     * Test that doEviction correctly evicts a term
     */
    @SneakyThrows
    public void test_doEviction_erasesTerm() {
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(cacheKey1, term1);
        List<DocumentCluster> expectedClusterList = prepareClusterList();
        PostingClusters expectedCluster = preparePostingClusters();
        ClusteredPostingCache.getInstance().get(cacheKey1).getWriter().insert(term1, expectedClusterList);

        // Verify the clustered posting cache has the term
        PostingClusters writtenCluster = ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1);
        assertSame(expectedClusterList, writtenCluster.getClusters());

        // Call doEviction
        long bytesFreed = testCache.doEviction(termKey);

        // Verify the term has been removed
        assertNull(ClusteredPostingCache.getInstance().get(cacheKey1).getReader().read(term1));

        // Verify the correct number of bytes was returned
        assertEquals(expectedCluster.ramBytesUsed() + RamUsageEstimator.shallowSizeOf(term1) + term1.bytes.length, bytesFreed);
    }

    /**
     * Test that doEviction does nothing when the key is not within the clustered posting cache
     */
    @SneakyThrows
    public void test_doEviction_withNonExistentKey() {
        CacheKey nonExistentCacheKey = new CacheKey(TestsPrepareUtils.prepareSegmentInfo(), "non_existent_field");
        LruTermCache.TermKey termKey = new LruTermCache.TermKey(nonExistentCacheKey, term1);

        long bytesFreed = testCache.doEviction(termKey);

        assertEquals(0, bytesFreed);
    }

    /**
     * Test that evict correctly evicts terms until enough memory is freed
     */
    @SneakyThrows
    public void test_evict_untilEnoughMemoryFreed() {
        TestLruTermCache testCacheSpy = spy(testCache);
        LruTermCache.TermKey termKey1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey termKey2 = new LruTermCache.TermKey(cacheKey1, term2);
        LruTermCache.TermKey termKey3 = new LruTermCache.TermKey(cacheKey2, term3);

        testCacheSpy.updateAccess(termKey1);
        testCacheSpy.updateAccess(termKey2);
        testCacheSpy.updateAccess(termKey3);

        when(testCacheSpy.doEviction(termKey1)).thenReturn(10L);
        when(testCacheSpy.doEviction(termKey2)).thenReturn(20L);
        when(testCacheSpy.doEviction(termKey3)).thenReturn(30L);

        testCacheSpy.evict(30L);

        // The third term with termKey3 should still be in the cache
        LruTermCache.TermKey remainingTerm = testCacheSpy.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());
    }

    /**
     * Test that removeIndex correctly removes all terms for an index
     */
    public void test_removeIndex_removesAllTermsForIndex() {
        // Add terms to the cache for different indices
        LruTermCache.TermKey termKey1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey termKey2 = new LruTermCache.TermKey(cacheKey1, term2);
        LruTermCache.TermKey termKey3 = new LruTermCache.TermKey(cacheKey2, term3);

        testCache.updateAccess(termKey1);
        testCache.updateAccess(termKey2);
        testCache.updateAccess(termKey3);

        // Remove all terms for cacheKey1
        testCache.removeIndex(cacheKey1);

        // Verify only terms for cacheKey2 remain
        LruTermCache.TermKey remainingTerm = testCache.getLeastRecentlyUsedItem();
        assertNotNull(remainingTerm);
        assertEquals(cacheKey2, remainingTerm.getCacheKey());
        assertEquals(term3, remainingTerm.getTerm());
    }

    /**
     * Test that removeIndex throws NullPointerException when key is null
     */
    public void test_removeIndex_withNullKey() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> testCache.removeIndex(null));
        assertEquals("cacheKey is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsTrue_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1, key2);
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsFalse_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1, key2);
    }

    /**
     * Test TermKey equals
     */
    public void test_TermKey_equals_returnsFalse_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1, key2);
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsEqualValues_whenSame() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentTerm() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey1, term2);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey hashCode
     */
    public void test_TermKey_hashCodeEquals_returnsDifferentValues_withDifferentCacheKey() {
        LruTermCache.TermKey key1 = new LruTermCache.TermKey(cacheKey1, term1);
        LruTermCache.TermKey key2 = new LruTermCache.TermKey(cacheKey2, term1);

        assertNotEquals(key1.hashCode(), key2.hashCode());
    }

    /**
     * Test TermKey get CacheKey
     */
    public void test_TermKey_getCacheKey() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(cacheKey1, key.getCacheKey());
    }

    /**
     * Test TermKey get Term
     */
    public void test_TermKey_getTerm() {
        LruTermCache.TermKey key = new LruTermCache.TermKey(cacheKey1, term1);

        assertEquals(term1, key.getTerm());
    }

    /**
     * Clear the LRU Term Cache and Clustered Posting Cache to avoid impact on other tests
     */
    @Override
    public void tearDown() throws Exception {
        testCache.clearAll();
        ClusteredPostingCache.getInstance().removeIndex(cacheKey1);
        super.tearDown();
    }

    private static class TestLruTermCache extends LruTermCache {

        public TestLruTermCache() {
            super();
        }

        public void clearAll() {
            super.evict(Long.MAX_VALUE);
        }
    }
}

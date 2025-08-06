/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.index.SegmentInfo;
import org.junit.After;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class CacheForwardIndexTests extends AbstractSparseTestBase {

    private static final String FIELD_NAME = "test_field";
    private static final int DOC_COUNT = 10;

    private SegmentInfo segmentInfo;
    private CacheKey.IndexKey indexKey;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        segmentInfo = mock(SegmentInfo.class);

        // Create an IndexKey for testing
        indexKey = new CacheKey.IndexKey(segmentInfo, FIELD_NAME);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        // Clean up any existing indices with this key
        ForwardIndexCacheManager.getInstance().removeIndex(indexKey);
        super.tearDown();
    }

    public void testGetOrCreate_withExistingKey() {
        // Create an index and then get it
        CacheForwardIndex createdIndex = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);
        CacheForwardIndex retrievedIndex = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);
        assertSame("Should return the same index instance", createdIndex, retrievedIndex);
    }

    public void testGetOrCreate_withNullKey() {
        // Test with null key
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            ForwardIndexCacheManager.getInstance().getOrCreate(null, DOC_COUNT);
        });
        assertEquals("Index key cannot be null", exception.getMessage());
    }

    public void testGet_withExistingKey() {
        // Create an index and then get it
        CacheForwardIndex createdIndex = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);
        CacheForwardIndex retrievedIndex = ForwardIndexCacheManager.getInstance().get(indexKey);
        assertSame("Should return the same index instance", createdIndex, retrievedIndex);
    }

    public void testGet_withNonExistingKey() {
        // Test getting a non-existent index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().get(indexKey);
        assertNull("Index should be null for non-existent key", index);
    }

    public void testGet_withNullKey() {
        // Test with null key
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            ForwardIndexCacheManager.getInstance().get(null);
        });
        assertEquals("Index key cannot be null", exception.getMessage());
    }

    public void testRemoveIndex() {
        // Create an index
        ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);
        assertNotNull("Index should exist", ForwardIndexCacheManager.getInstance().get(indexKey));

        // Remove the index
        ForwardIndexCacheManager.getInstance().removeIndex(indexKey);
        assertNull("Index should be removed", ForwardIndexCacheManager.getInstance().get(indexKey));

        // Test removing a non-existent index (should not throw)
        ForwardIndexCacheManager.getInstance().removeIndex(indexKey);
    }

    public void testReadInsert_withValidVector() throws IOException {
        // Create an index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Get reader and writer
        SparseVectorReader reader = index.getReader();
        SparseVectorWriter writer = index.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < DOC_COUNT; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        // Insert a vector
        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(0, vector1);

        // Read the vector back
        SparseVector readVector1 = reader.read(0);
        assertEquals("Read vector should match inserted vector", vector1, readVector1);

        // Test inserting to an out-of-bounds docId
        writer.insert(DOC_COUNT + 1, vector1); // Should be ignored, no exception
    }

    public void testReadInsert_withNullVector() throws IOException {
        // Create an index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Get reader and writer
        SparseVectorReader reader = index.getReader();
        SparseVectorWriter writer = index.getWriter();

        // Test initial state - all vectors should be null
        for (int i = 0; i < DOC_COUNT; i++) {
            assertNull("Vector should be null initially", reader.read(i));
        }

        // Test inserting null vector
        writer.insert(2, null); // Should be ignored, no exception
        assertNull("Vector should still be null", reader.read(2));
    }

    public void testInsert_skipsDuplicates() throws IOException {
        // Create an index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Get reader and writer
        SparseVectorReader reader = index.getReader();
        SparseVectorWriter writer = index.getWriter();

        // Insert first vector
        SparseVector vector1 = createVector(1, 2, 3, 4);
        writer.insert(0, vector1);
        assertEquals("First vector should be inserted", vector1, reader.read(0));

        // Try to insert different vector at same position
        SparseVector vector2 = createVector(5, 6, 7, 8);
        writer.insert(0, vector2);
        assertEquals("Original vector should remain unchanged", vector1, reader.read(0));
    }

    public void testRamBytesUsed() throws IOException {
        // Create an empty index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Initial RAM usage should be positive (array overhead)
        long initialRam = index.ramBytesUsed();
        assertTrue("Empty index should use some RAM for array", initialRam > 0);

        // Add some vectors
        SparseVectorWriter writer = index.getWriter();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        SparseVector vector2 = createVector(5, 6, 7, 8, 9, 10);
        writer.insert(0, vector1);
        writer.insert(1, vector2);

        // RAM usage should increase
        long ramWithVectors = index.ramBytesUsed();
        assertTrue("RAM usage should increase after adding vectors", ramWithVectors > initialRam);
    }

    public void testTotalMemoryUsed() throws IOException {
        // Create an empty index
        CacheForwardIndex index = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Initial Memory usage should be positive
        long initialMemUsage = ForwardIndexCacheManager.getInstance().ramBytesUsed();
        assertTrue("Initial memory usage should be positive", initialMemUsage > 0);

        // Add some vectors
        SparseVectorWriter writer = index.getWriter();
        SparseVector vector1 = createVector(1, 2, 3, 4);
        SparseVector vector2 = createVector(5, 6, 7, 8, 9, 10);
        writer.insert(0, vector1);
        writer.insert(1, vector2);

        // Test memUsage static method
        long totalMemUsage = ForwardIndexCacheManager.getInstance().ramBytesUsed();
        assertTrue("Total memory usage should increase", totalMemUsage > initialMemUsage);
    }

    public void testMultipleIndices() {
        // Create first index
        CacheForwardIndex index1 = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey, DOC_COUNT);

        // Create a second index with a different key
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, "another_field");
        CacheForwardIndex index2 = ForwardIndexCacheManager.getInstance().getOrCreate(indexKey2, DOC_COUNT);

        // Verify they are different instances
        assertNotSame("Should be different index instances", index1, index2);
    }
}

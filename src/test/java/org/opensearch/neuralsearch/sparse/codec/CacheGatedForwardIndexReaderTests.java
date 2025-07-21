/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import java.io.IOException;

import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.common.SparseVectorReader;
import org.opensearch.neuralsearch.sparse.common.SparseVectorWriter;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CacheGatedForwardIndexReaderTests extends AbstractSparseTestBase {

    private final int testDocId = 1;
    private final SparseVector testSparseVector = createVector(1, 5, 2, 3);
    private final SparseVectorReader inMemoryReader = mock(SparseVectorReader.class);
    private final SparseVectorWriter inMemoryWriter = mock(SparseVectorWriter.class);
    private final SparseVectorReader luceneReader = mock(SparseVectorReader.class);

    /**
     * Tests the constructor of CacheGatedForwardIndexReader when null is passed for inMemoryReader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testCacheGatedForwardIndexReaderConstructorWithNullInMemoryReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedForwardIndexReader(null, inMemoryWriter, luceneReader);
        });
        assertEquals("inMemoryReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedForwardIndexReader when null is passed for inMemoryWriter.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testCacheGatedForwardIndexReaderConstructorWithNullInMemoryWriter() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedForwardIndexReader(inMemoryReader, null, luceneReader);
        });
        assertEquals("inMemoryWriter is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedForwardIndexReader when null is passed for luceneReader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testCacheGatedForwardIndexReaderConstructorWithNullLuceneReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedForwardIndexReader(inMemoryReader, inMemoryWriter, null);
        });
        assertEquals("luceneReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test case for the CacheGatedForwardIndexReader constructor.
     * Verifies that the constructor successfully creates an instance
     * when provided with valid non-null parameters.
     */
    public void test_CacheGatedForwardIndexReader_ConstructorWithValidParameters() {
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(inMemoryReader, inMemoryWriter, luceneReader);
        assertNotNull("CacheGatedForwardIndexReader should be created successfully", reader);
    }

    /**
     * Test case for the read method when the vector is found in the in-memory cache.
     * This test verifies that the method returns the vector from the cache without accessing Lucene storage.
     */
    public void testReadVectorFromCache() throws IOException {
        when(inMemoryReader.read(anyInt())).thenReturn(testSparseVector);

        // Create the CacheGatedForwardIndexReader instance
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(inMemoryReader, inMemoryWriter, luceneReader);

        // Call the method under test
        SparseVector result = reader.read(testDocId);

        // Verify the result
        assertEquals(testSparseVector, result);

        // Verify that luceneReader was not called
        verify(luceneReader, never()).read(anyInt());
    }

    /**
     * Tests the read method when both in-memory cache and Lucene storage return null.
     * This scenario verifies that the method correctly handles the case where the
     * requested vector does not exist in either storage.
     */
    public void testReadWhenVectorDoesNotExist() throws IOException {
        when(inMemoryReader.read(anyInt())).thenReturn(null);
        when(luceneReader.read(anyInt())).thenReturn(null);

        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(inMemoryReader, inMemoryWriter, luceneReader);

        SparseVector result = reader.read(testDocId);

        assertNull(result);
        verify(inMemoryReader).read(testDocId);
        verify(luceneReader).read(testDocId);
        verify(inMemoryWriter, never()).insert(anyInt(), any(SparseVector.class));
    }

    /**
     * Test case for read method when the vector is not in memory cache but exists in Lucene storage.
     * This test verifies that:
     * 1. The method attempts to read from in-memory cache first (which returns null)
     * 2. Then reads from Lucene storage successfully
     * 3. The retrieved vector is inserted into the in-memory cache
     * 4. The method returns the vector retrieved from Lucene storage
     *
     * @throws IOException if an I/O error occurs during the test
     */
    public void test_read_whenVectorNotInCacheButInLucene() throws IOException {
        when(inMemoryReader.read(anyInt())).thenReturn(null);
        when(luceneReader.read(anyInt())).thenReturn(testSparseVector);

        // Create the CacheGatedForwardIndexReader instance
        CacheGatedForwardIndexReader reader = new CacheGatedForwardIndexReader(inMemoryReader, inMemoryWriter, luceneReader);

        // Execute the method under test
        SparseVector result = reader.read(testDocId);

        // Verify the result
        assertEquals(testSparseVector, result);

        // Verify that the vector was inserted into the in-memory cache
        verify(inMemoryWriter).insert(testDocId, testSparseVector);
    }
}

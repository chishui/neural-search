/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingReader;
import org.opensearch.neuralsearch.sparse.common.ClusteredPostingWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CacheGatedPostingsReaderTests extends AbstractSparseTestBase {

    private final String testFieldName = "test_field";
    private final BytesRef testTerm = new BytesRef("test_term");
    private final ClusteredPostingReader inMemoryReader = mock(ClusteredPostingReader.class);
    private final ClusteredPostingWriter inMemoryWriter = mock(ClusteredPostingWriter.class);
    private final SparseTermsLuceneReader luceneReader = mock(SparseTermsLuceneReader.class);
    private final PostingClusters testPostingClusters = createTestPostingClusters();

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for fieldName.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testConstructorWithNullFieldName() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(null, inMemoryReader, inMemoryWriter, luceneReader);
        });
        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for inMemoryReader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testConstructorWithNullInMemoryReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, null, inMemoryWriter, luceneReader);
        });
        assertEquals("inMemoryReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for inMemoryWriter.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testConstructorWithNullInMemoryWriter() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, inMemoryReader, null, luceneReader);
        });
        assertEquals("inMemoryWriter is marked non-null but is null", exception.getMessage());
    }

    /**
     * Tests the constructor of CacheGatedPostingsReader when null is passed for luceneReader.
     * This should throw a NullPointerException as specified in the method's documentation.
     */
    public void testConstructorWithNullLuceneReader() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, null);
        });
        assertEquals("luceneReader is marked non-null but is null", exception.getMessage());
    }

    /**
     * Test case for the CacheGatedPostingsReader constructor.
     * Verifies that the constructor successfully creates an instance
     * when provided with valid non-null parameters.
     */
    public void testConstructorWithValidParameters() {
        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        assertNotNull("CacheGatedPostingsReader should be created successfully", reader);
    }

    /**
     * Test case for the read method when the posting clusters are found in the in-memory cache.
     * This test verifies that the method returns the clusters from the cache without accessing Lucene storage.
     */
    public void testReadFromCache() throws IOException {
        when(inMemoryReader.read(any(BytesRef.class))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertEquals(testPostingClusters, result);
        verify(inMemoryReader).read(testTerm);
        verify(luceneReader, never()).read(anyString(), any(BytesRef.class));
        verify(inMemoryWriter, never()).insert(any(BytesRef.class), any());
    }

    /**
     * Tests the read method when both in-memory cache and Lucene storage return null.
     * This scenario verifies that the method correctly handles the case where the
     * requested posting clusters do not exist in either storage.
     */
    public void testReadWhenPostingClustersDoNotExist() throws IOException {
        when(inMemoryReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenReturn(null);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertNull(result);
        verify(inMemoryReader).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(inMemoryWriter, never()).insert(any(BytesRef.class), any());
    }

    /**
     * Test case for read method when the posting clusters are not in memory cache but exist in Lucene storage.
     * This test verifies that:
     * 1. The method attempts to read from in-memory cache first (which returns null)
     * 2. Then reads from Lucene storage successfully
     * 3. The retrieved posting clusters are inserted into the in-memory cache
     * 4. The method returns the posting clusters retrieved from Lucene storage
     */
    public void testReadWhenNotInCacheButInLucene() throws IOException {
        when(inMemoryReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        PostingClusters result = reader.read(testTerm);

        assertEquals(testPostingClusters, result);
        verify(inMemoryReader).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(inMemoryWriter).insert(eq(testTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the getTerms method to verify it returns terms from the Lucene reader.
     * This test ensures that the method correctly delegates to the Lucene reader
     * rather than using the in-memory cache, which may be incomplete.
     */
    public void testGetTerms() {
        Set<BytesRef> expectedTerms = new HashSet<>();
        expectedTerms.add(new BytesRef("term1"));
        expectedTerms.add(new BytesRef("term2"));

        when(luceneReader.getTerms(anyString())).thenReturn(expectedTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        Set<BytesRef> result = reader.getTerms();

        assertEquals(expectedTerms, result);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the size method to verify it returns the correct number of terms.
     * This test ensures that the method correctly calculates the size based on
     * the number of terms in the Lucene reader.
     */
    public void testSize() {
        Set<BytesRef> terms = new HashSet<>();
        terms.add(new BytesRef("term1"));
        terms.add(new BytesRef("term2"));
        terms.add(new BytesRef("term3"));

        when(luceneReader.getTerms(anyString())).thenReturn(terms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        long size = reader.size();

        assertEquals(3, size);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the read method with an empty term.
     * This test verifies that the method handles empty terms correctly.
     */
    public void testReadWithEmptyTerm() throws IOException {
        BytesRef emptyTerm = new BytesRef("");
        when(inMemoryReader.read(emptyTerm)).thenReturn(null);
        when(luceneReader.read(anyString(), eq(emptyTerm))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        PostingClusters result = reader.read(emptyTerm);

        assertEquals(testPostingClusters, result);
        verify(inMemoryReader).read(emptyTerm);
        verify(luceneReader).read(testFieldName, emptyTerm);
        verify(inMemoryWriter).insert(eq(emptyTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the getTerms method when the Lucene reader returns an empty set.
     * This test ensures that the method correctly handles the case where there are no terms.
     */
    public void testGetTermsWithEmptySet() {
        Set<BytesRef> emptyTerms = new HashSet<>();
        when(luceneReader.getTerms(anyString())).thenReturn(emptyTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        Set<BytesRef> result = reader.getTerms();

        assertTrue(result.isEmpty());
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the size method when there are no terms.
     * This test ensures that the method correctly returns zero when there are no terms.
     */
    public void testSizeWithNoTerms() {
        Set<BytesRef> emptyTerms = new HashSet<>();
        when(luceneReader.getTerms(anyString())).thenReturn(emptyTerms);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        long size = reader.size();

        assertEquals(0, size);
        verify(luceneReader).getTerms(testFieldName);
    }

    /**
     * Tests the read method with a term that has special characters.
     * This test verifies that the method handles terms with special characters correctly.
     */
    public void testReadWithSpecialCharacterTerm() throws IOException {
        BytesRef specialTerm = new BytesRef("special!@#$%^&*()_+");
        when(inMemoryReader.read(specialTerm)).thenReturn(null);
        when(luceneReader.read(anyString(), eq(specialTerm))).thenReturn(testPostingClusters);

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);
        PostingClusters result = reader.read(specialTerm);

        assertEquals(testPostingClusters, result);
        verify(inMemoryReader).read(specialTerm);
        verify(luceneReader).read(testFieldName, specialTerm);
        verify(inMemoryWriter).insert(eq(specialTerm), eq(testPostingClusters.getClusters()));
    }

    /**
     * Tests the read method when an IOException occurs while reading from Lucene.
     * This test verifies that the method properly propagates the exception.
     */
    public void testReadWithIOException() throws IOException {
        when(inMemoryReader.read(any(BytesRef.class))).thenReturn(null);
        when(luceneReader.read(anyString(), any(BytesRef.class))).thenThrow(new IOException("Test IO Exception"));

        CacheGatedPostingsReader reader = new CacheGatedPostingsReader(testFieldName, inMemoryReader, inMemoryWriter, luceneReader);

        IOException exception = expectThrows(IOException.class, () -> {
            reader.read(testTerm);
        });

        assertEquals("Test IO Exception", exception.getMessage());
        verify(inMemoryReader).read(testTerm);
        verify(luceneReader).read(testFieldName, testTerm);
        verify(inMemoryWriter, never()).insert(any(BytesRef.class), any());
    }
}

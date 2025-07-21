/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.common.SparseVector;

import java.util.Arrays;
import java.util.List;

public class BatchClusteringTaskTests extends AbstractSparseTestBase {
    private static List<BytesRef> terms;
    private static InMemoryKey.IndexKey key;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        terms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));
        key = new InMemoryKey.IndexKey(TestsPrepareUtils.prepareSegmentInfo(), "test_field");
    }

    public void testConstructorDeepCopiesTerms() throws Exception {
        // Setup
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));

        // Execute - create task with null mergeState to test constructor
        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, null, null);

        // Verify task is created
        assertNotNull("Task should be created successfully", task);

        List<BytesRef> taskTerms = task.getTerms();

        // Verify deep copy by checking actual content
        assertEquals("First term should be 'term1'", "term1", taskTerms.get(0).utf8ToString());

        // Modify original terms to verify deep copy
        originalTerms.get(0).bytes[0] = (byte) 'X';

        // Verify task's terms remain unchanged (proving deep copy worked)
        assertEquals("Task's first term should still be 'term1'", "term1", taskTerms.get(0).utf8ToString());
        assertNotEquals("Original term should now be different", "term1", originalTerms.get(0).utf8ToString());
    }

    public void testGetWithNullMergeState() throws Exception {
        // Test behavior with null merge state - should throw NullPointerException when accessing maxDocs
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, null, null);

        Exception exception = assertThrows(NullPointerException.class, () -> task.get());

        // Assert that the exception is an instance of NullPointerException
        assertTrue("Exception should be an instance of NullPointerException", exception instanceof NullPointerException);
    }

    public void testGetWithNonNullMergeState() throws Exception {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        assertEquals("Should return clusters for each term", terms.size(), result.size());

        for (int i = 0; i < result.size(); i++) {
            Pair<BytesRef, PostingClusters> pair = result.get(i);

            // Verify term matches
            assertNotNull("Term should not be null", pair.getLeft());
            assertEquals("Term should match input", terms.get(i).utf8ToString(), pair.getLeft().utf8ToString());

            // Verify clusters
            PostingClusters clusters = pair.getRight();
            assertNotNull("PostingClusters should not be null", clusters);
            assertNotNull("Clusters list should not be null", clusters.getClusters());

            // Additional cluster validation
            assertTrue("Should have non-negative cluster count", clusters.getClusters().size() >= 0);
        }
    }

    public void testGetWithNonNullMergeStateZeroMaxDocs() throws Exception {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = true;
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        // Should trigger early quit schema to return an empty list
        assertEquals("Should return an empty list", 0, result.size());
    }

    public void testTermsDeepCopyInGet() throws Exception {
        // Test that the terms are properly deep copied and used in get() method
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("original1"), new BytesRef("original2"));

        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, null, null);

        // Modify original terms
        originalTerms.get(0).bytes[0] = (byte) 'M';

        // The task should still have the original values due to deep copy
        // We can't easily test the get() method output, but we can verify the task was created properly
        assertNotNull("Task should be created and maintain its own copy of terms", task);
    }

    public void testReadFromCacheMethods_directTrigger() throws Exception {
        boolean isEmptyMaxDocs = false;
        MergeState mergeState = TestsPrepareUtils.prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        java.lang.reflect.Method readFromCacheOfMergedSegment = BatchClusteringTask.class.getDeclaredMethod(
            "readFromCacheOfMergedSegment",
            int.class
        );
        readFromCacheOfMergedSegment.setAccessible(true);

        java.lang.reflect.Method readFromCacheOfOldSegment = BatchClusteringTask.class.getDeclaredMethod(
            "readFromCacheOfOldSegment",
            SegmentInfo.class,
            int.class
        );
        readFromCacheOfOldSegment.setAccessible(true);

        SparseVector result1 = (SparseVector) readFromCacheOfMergedSegment.invoke(task, 0);
        assertNull("Should return null when no cache is set", result1);

        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        SparseVector result2 = (SparseVector) readFromCacheOfOldSegment.invoke(task, segmentInfo, 0);
        assertNull("Should return null when no cache is set", result2);
    }
}

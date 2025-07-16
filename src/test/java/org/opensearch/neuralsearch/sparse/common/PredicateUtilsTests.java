/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;

public class PredicateUtilsTests extends AbstractSparseTestBase {

    public void testShouldRunSeisPredicate_withDocCountAboveThreshold_returnsTrue() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        fieldInfo.putAttribute(ALGO_TRIGGER_DOC_COUNT_FIELD, "5");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) >= threshold (5)
        assertTrue("Should return true when doc count is above threshold", result);
    }

    public void testShouldRunSeisPredicate_withDocCountEqualToThreshold_returnsTrue() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        fieldInfo.putAttribute(ALGO_TRIGGER_DOC_COUNT_FIELD, "10");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) >= threshold (10)
        assertTrue("Should return true when doc count equals threshold", result);
    }

    public void testShouldRunSeisPredicate_withDocCountBelowThreshold_returnsFalse() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        fieldInfo.putAttribute(ALGO_TRIGGER_DOC_COUNT_FIELD, "15");

        // Execute
        boolean result = PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);

        // Verify - segmentInfo.maxDoc() (10) < threshold (15)
        assertFalse("Should return false when doc count is below threshold", result);
    }

    public void testShouldRunSeisPredicate_withInvalidThreshold_throwsException() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        fieldInfo.putAttribute(ALGO_TRIGGER_DOC_COUNT_FIELD, "invalid_number");

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertNotNull("Should throw NumberFormatException for invalid threshold", exception);
    }

    public void testShouldRunSeisPredicate_withMissingAttribute_throwsException() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        // No ALGO_TRIGGER_DOC_COUNT_FIELD

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertNotNull("Should throw NumberFormatException when attribute is missing", exception);
    }

    public void testShouldRunSeisPredicate_withNullAttribute_throwsException() {
        // Setup
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo(); // maxDoc = 10
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        fieldInfo.putAttribute(ALGO_TRIGGER_DOC_COUNT_FIELD, null);

        // Execute and verify exception
        NumberFormatException exception = expectThrows(NumberFormatException.class, () -> {
            PredicateUtils.shouldRunSeisPredicate.test(segmentInfo, fieldInfo);
        });

        assertNotNull("Should throw NumberFormatException when attribute is null", exception);
    }

    public void testShouldRunSeisPredicate_isNotNull() {
        assertNotNull("shouldRunSeisPredicate should not be null", PredicateUtils.shouldRunSeisPredicate);
    }

    public void testShouldRunSeisPredicate_isBiPredicate() {
        assertTrue(
            "shouldRunSeisPredicate should be instance of BiPredicate",
            PredicateUtils.shouldRunSeisPredicate instanceof java.util.function.BiPredicate
        );
    }
}

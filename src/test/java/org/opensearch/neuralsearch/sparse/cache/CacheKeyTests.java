/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

public class CacheKeyTests extends AbstractSparseTestBase {

    private static SegmentInfo segmentInfo;
    private static FieldInfo fieldInfo;
    private static String fieldName;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        fieldName = "test_field";
    }

    public void testCacheKey_constructorWithFieldInfo_createsCorrectly() {
        CacheKey cacheKey = new CacheKey(segmentInfo, fieldInfo);

        assertNotNull("CacheKey should be created", cacheKey);
    }

    public void testCacheKey_constructorWithFieldName_createsCorrectly() {
        CacheKey cacheKey = new CacheKey(segmentInfo, fieldName);

        assertNotNull("CacheKey should be created", cacheKey);
    }

    public void testCacheKey_constructorWithNullSegmentInfoLegalFieldInfo_createsCorrectly() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, fieldInfo); });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testCacheKey_constructorWithNullSegmentInfoLegalString_createsCorrectly() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, fieldName); });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testCacheKey_constructorWithNullFieldName_createsCorrectly() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(segmentInfo, null); });
        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    public void testCacheKey_constructorWithNullFieldInfo_createsCorrectly() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(segmentInfo, null); });
        assertEquals("fieldInfo is marked non-null but is null", exception.getMessage());
    }

    public void testCacheKey_constructorWithBothNullFieldInfo_createsCorrectly() {
        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey(null, null); });
        // Trigger first parameter NonNull check
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testFieldInfo_equals_withSameValues_returnsTrue() {
        CacheKey cacheKey1 = new CacheKey(segmentInfo, fieldName);
        CacheKey cacheKey2 = new CacheKey(segmentInfo, fieldName);

        assertEquals("CacheKeys with same values should be equal", cacheKey1, cacheKey2);
    }

    public void testCacheKey_equals_withDifferentSegmentInfo_returnsFalse() {
        SegmentInfo segmentInfo1 = TestsPrepareUtils.prepareSegmentInfo();
        SegmentInfo segmentInfo2 = TestsPrepareUtils.prepareSegmentInfo();

        CacheKey cacheKey1 = new CacheKey(segmentInfo1, fieldName);
        CacheKey cacheKey2 = new CacheKey(segmentInfo2, fieldName);

        assertNotEquals("CacheKeys with different SegmentInfo should not be equal", cacheKey1, cacheKey2);
    }

    public void testCacheKey_equals_withDifferentFieldName_returnsFalse() {
        CacheKey cacheKey1 = new CacheKey(segmentInfo, "field1");
        CacheKey cacheKey2 = new CacheKey(segmentInfo, "field2");

        assertNotEquals("CacheKeys with different field names should not be equal", cacheKey1, cacheKey2);
    }

    public void testCacheKey_equals_withSameInstance_returnsTrue() {
        CacheKey cacheKey = new CacheKey(segmentInfo, "test_field");

        assertEquals("CacheKey should equal itself", cacheKey, cacheKey);
    }

    public void testCacheKey_equals_withNull_returnsFalse() {
        CacheKey cacheKey = new CacheKey(segmentInfo, "test_field");

        assertNotEquals("CacheKey should not equal null", cacheKey, null);
    }

    public void testCacheKey_equals_withDifferentClass_returnsFalse() {
        CacheKey cacheKey = new CacheKey(segmentInfo, "test_field");

        assertNotEquals("CacheKey should not equal different class", cacheKey, "string");
    }

    public void testCacheKey_hashCode_withSameValues_returnsSameHashCode() {
        CacheKey cacheKey1 = new CacheKey(segmentInfo, fieldName);
        CacheKey cacheKey2 = new CacheKey(segmentInfo, fieldName);

        assertEquals("CacheKeys with same values should have same hash code", cacheKey1.hashCode(), cacheKey2.hashCode());
    }

    public void testCacheKey_hashCode_withDifferentValues_returnsDifferentHashCode() {
        CacheKey cacheKey1 = new CacheKey(segmentInfo, "field1");
        CacheKey cacheKey2 = new CacheKey(segmentInfo, "field2");

        assertNotEquals("CacheKeys with different values should have different hash codes", cacheKey1.hashCode(), cacheKey2.hashCode());
    }

    public void testCacheKey_constructorWithFieldInfo_extractsFieldName() {
        CacheKey cacheKey1 = new CacheKey(segmentInfo, fieldInfo);
        CacheKey cacheKey2 = new CacheKey(segmentInfo, "test_field");

        assertEquals("CacheKey created with FieldInfo should equal CacheKey created with field name", cacheKey1, cacheKey2);
    }

    public void testCacheKey_canBeInstantiated() {
        CacheKey cacheKey = new CacheKey();
        assertNotNull("CacheKey should be instantiable", cacheKey);
    }
}

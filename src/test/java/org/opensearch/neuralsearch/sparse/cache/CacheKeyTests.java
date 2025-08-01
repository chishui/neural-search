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

    public void testIndexKey_constructorWithFieldInfo_createsCorrectly() {

        CacheKey.IndexKey indexKey = new CacheKey.IndexKey(segmentInfo, fieldInfo);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithFieldName_createsCorrectly() {

        CacheKey.IndexKey indexKey = new CacheKey.IndexKey(segmentInfo, fieldName);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithNullSegmentInfoLegalFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey.IndexKey(null, fieldInfo); });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullSegmentInfoLegalString_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> { new CacheKey.IndexKey(null, fieldName); });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullFieldName_createsCorrectly() {

        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> { new CacheKey.IndexKey(segmentInfo, (String) null); }
        );
        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> { new CacheKey.IndexKey(segmentInfo, (FieldInfo) null); }
        );
        assertEquals("fieldInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithBothNullFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new CacheKey.IndexKey((SegmentInfo) null, (FieldInfo) null);
        });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage()); // Trigger first parameter NonNull check
    }

    public void testIndexKey_equals_withSameValues_returnsTrue() {

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo, fieldName);
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentSegmentInfo_returnsFalse() {
        SegmentInfo segmentInfo1 = TestsPrepareUtils.prepareSegmentInfo();
        SegmentInfo segmentInfo2 = TestsPrepareUtils.prepareSegmentInfo();

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo1, fieldName);
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo2, fieldName);

        assertNotEquals("IndexKeys with different SegmentInfo should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentFieldName_returnsFalse() {

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo, "field1");
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different field names should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withSameInstance_returnsTrue() {
        CacheKey.IndexKey indexKey = new CacheKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey should equal itself", indexKey, indexKey);
    }

    public void testIndexKey_equals_withNull_returnsFalse() {
        CacheKey.IndexKey indexKey = new CacheKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal null", indexKey, null);
    }

    public void testIndexKey_equals_withDifferentClass_returnsFalse() {
        CacheKey.IndexKey indexKey = new CacheKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal different class", indexKey, "string");
    }

    public void testIndexKey_hashCode_withSameValues_returnsSameHashCode() {

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo, fieldName);
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should have same hash code", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_hashCode_withDifferentValues_returnsDifferentHashCode() {

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo, "field1");
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different values should have different hash codes", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_constructorWithFieldInfo_extractsFieldName() {

        CacheKey.IndexKey indexKey1 = new CacheKey.IndexKey(segmentInfo, fieldInfo);
        CacheKey.IndexKey indexKey2 = new CacheKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey created with FieldInfo should equal IndexKey created with field name", indexKey1, indexKey2);
    }

    public void testCacheKey_canBeInstantiated() {
        CacheKey cacheKey = new CacheKey();
        assertNotNull("CacheKey should be instantiable", cacheKey);
    }
}

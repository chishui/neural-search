/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

public class InMemoryKeyTests extends AbstractSparseTestBase {

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

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, fieldInfo);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithFieldName_createsCorrectly() {

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithNullSegmentInfoLegalFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(null, fieldInfo);
        });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullSegmentInfoLegalString_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(null, fieldName);
        });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullFieldName_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, (String) null);
        });
        assertEquals("fieldName is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithNullFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, (FieldInfo) null);
        });
        assertEquals("fieldInfo is marked non-null but is null", exception.getMessage());
    }

    public void testIndexKey_constructorWithBothNullFieldInfo_createsCorrectly() {

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey((SegmentInfo) null, (FieldInfo) null);
        });
        assertEquals("segmentInfo is marked non-null but is null", exception.getMessage()); // Trigger first parameter NonNull check
    }

    public void testIndexKey_equals_withSameValues_returnsTrue() {

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentSegmentInfo_returnsFalse() {
        SegmentInfo segmentInfo1 = TestsPrepareUtils.prepareSegmentInfo();
        SegmentInfo segmentInfo2 = TestsPrepareUtils.prepareSegmentInfo();

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo1, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo2, fieldName);

        assertNotEquals("IndexKeys with different SegmentInfo should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentFieldName_returnsFalse() {

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, "field1");
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different field names should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withSameInstance_returnsTrue() {
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey should equal itself", indexKey, indexKey);
    }

    public void testIndexKey_equals_withNull_returnsFalse() {
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal null", indexKey, null);
    }

    public void testIndexKey_equals_withDifferentClass_returnsFalse() {
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal different class", indexKey, "string");
    }

    public void testIndexKey_hashCode_withSameValues_returnsSameHashCode() {

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should have same hash code", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_hashCode_withDifferentValues_returnsDifferentHashCode() {

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, "field1");
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different values should have different hash codes", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_constructorWithFieldInfo_extractsFieldName() {

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldInfo);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey created with FieldInfo should equal IndexKey created with field name", indexKey1, indexKey2);
    }

    public void testInMemoryKey_canBeInstantiated() {
        InMemoryKey inMemoryKey = new InMemoryKey();
        assertNotNull("InMemoryKey should be instantiable", inMemoryKey);
    }
}

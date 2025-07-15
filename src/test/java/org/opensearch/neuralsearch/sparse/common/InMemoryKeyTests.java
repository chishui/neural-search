/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

public class InMemoryKeyTests extends AbstractSparseTestBase {

    public void testIndexKey_constructorWithFieldInfo_createsCorrectly() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, fieldInfo);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithFieldName_createsCorrectly() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        String fieldName = "test_field";

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertNotNull("IndexKey should be created", indexKey);
    }

    public void testIndexKey_constructorWithNullSegmentInfo_createsCorrectly() {
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(null, fieldInfo);

        assertNotNull("IndexKey should be created with null SegmentInfo", indexKey);
    }

    public void testIndexKey_constructorWithNullFieldName_createsCorrectly() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, (String) null);

        assertNotNull("IndexKey should be created with null field name", indexKey);
    }

    public void testIndexKey_equals_withSameValues_returnsTrue() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        String fieldName = "test_field";

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentSegmentInfo_returnsFalse() {
        SegmentInfo segmentInfo1 = TestsPrepareUtils.prepareSegmentInfo();
        SegmentInfo segmentInfo2 = TestsPrepareUtils.prepareSegmentInfo();
        String fieldName = "test_field";

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo1, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo2, fieldName);

        assertNotEquals("IndexKeys with different SegmentInfo should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withDifferentFieldName_returnsFalse() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, "field1");
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different field names should not be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_equals_withSameInstance_returnsTrue() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey should equal itself", indexKey, indexKey);
    }

    public void testIndexKey_equals_withNull_returnsFalse() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal null", indexKey, null);
    }

    public void testIndexKey_equals_withDifferentClass_returnsFalse() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        InMemoryKey.IndexKey indexKey = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertNotEquals("IndexKey should not equal different class", indexKey, "string");
    }

    public void testIndexKey_hashCode_withSameValues_returnsSameHashCode() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        String fieldName = "test_field";

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldName);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, fieldName);

        assertEquals("IndexKeys with same values should have same hash code", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_hashCode_withDifferentValues_returnsDifferentHashCode() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, "field1");
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "field2");

        assertNotEquals("IndexKeys with different values should have different hash codes", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_constructorWithFieldInfo_extractsFieldName() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        FieldInfo fieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(segmentInfo, fieldInfo);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, "test_field");

        assertEquals("IndexKey created with FieldInfo should equal IndexKey created with field name", indexKey1, indexKey2);
    }

    public void testIndexKey_withNullValues_handlesEqualsCorrectly() {
        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(null, (String) null);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(null, (String) null);

        assertEquals("IndexKeys with null values should be equal", indexKey1, indexKey2);
    }

    public void testIndexKey_withNullValues_handlesHashCodeCorrectly() {
        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(null, (String) null);
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(null, (String) null);

        assertEquals("IndexKeys with null values should have same hash code", indexKey1.hashCode(), indexKey2.hashCode());
    }

    public void testIndexKey_mixedNullValues_handlesCorrectly() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();

        InMemoryKey.IndexKey indexKey1 = new InMemoryKey.IndexKey(null, "field");
        InMemoryKey.IndexKey indexKey2 = new InMemoryKey.IndexKey(segmentInfo, (String) null);
        InMemoryKey.IndexKey indexKey3 = new InMemoryKey.IndexKey(null, "field");

        assertEquals("IndexKeys with same null/non-null pattern should be equal", indexKey1, indexKey3);
        assertNotEquals("IndexKeys with different null patterns should not be equal", indexKey1, indexKey2);
    }

    public void testInMemoryKey_canBeInstantiated() {
        InMemoryKey inMemoryKey = new InMemoryKey();
        assertNotNull("InMemoryKey should be instantiable", inMemoryKey);
    }
}

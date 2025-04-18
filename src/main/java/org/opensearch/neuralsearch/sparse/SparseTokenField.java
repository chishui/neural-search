/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableFieldType;

public class SparseTokenField extends Field {
    private float tokenValue;

    public SparseTokenField(String fieldName, String key, float value, IndexableFieldType type) {
        super(fieldName, type);
        this.tokenValue = value;
        this.fieldsData = key;
    }
}

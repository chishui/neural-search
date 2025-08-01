/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;

/**
 * Key for cache sparse vector forward index
 */
public class CacheKey {

    @EqualsAndHashCode
    public static class IndexKey {
        private final SegmentInfo segmentInfo;
        private final String field;

        public IndexKey(@NonNull SegmentInfo segmentInfo, @NonNull FieldInfo fieldInfo) {
            this.segmentInfo = segmentInfo;
            this.field = fieldInfo.name;
        }

        public IndexKey(@NonNull SegmentInfo segmentInfo, @NonNull String fieldName) {
            this.segmentInfo = segmentInfo;
            this.field = fieldName;
        }
    }
}

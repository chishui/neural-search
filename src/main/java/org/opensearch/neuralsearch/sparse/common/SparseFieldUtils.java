/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SparseFieldUtils {

    // no instance of this util class
    private SparseFieldUtils() {}

    @SuppressWarnings("unchecked")
    public static Set<String> getSparseAnnFields(String index) {
        if (index == null) {
            return Collections.EMPTY_SET;
        }
        final IndexMetadata metadata = NeuralSearchClusterUtil.instance().getClusterService().state().metadata().index(index);
        if (metadata == null || !SparseSettings.IS_SPARSE_INDEX_SETTING.get(metadata.getSettings())) {
            return Collections.EMPTY_SET;
        }
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null || mappingMetadata.sourceAsMap() == null) {
            return Collections.EMPTY_SET;
        }
        Object properties = mappingMetadata.sourceAsMap().get("properties");
        if (!(properties instanceof Map)) {
            return Collections.EMPTY_SET;
        }
        Set<String> sparseAnnFields = new HashSet<>();
        Map<String, Object> fields = (Map<String, Object>) properties;
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            Map<String, Object> fieldMap = (Map<String, Object>) field.getValue();
            Object type = fieldMap.get("type");
            if (Objects.nonNull(type) && SparseTokensFieldType.isSparseTokensType(type.toString())) {
                sparseAnnFields.add(field.getKey());
            }
        }
        return sparseAnnFields;
    }
}

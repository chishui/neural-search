/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.opensearch.common.settings.Setting;

import static org.opensearch.common.settings.Setting.Property.Final;
import static org.opensearch.common.settings.Setting.Property.IndexScope;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SparseSettings {
    public static final String SPARSE_INDEX = "index.sparse";

    private static SparseSettings INSTANCE;

    public static synchronized SparseSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new SparseSettings();
        }
        return INSTANCE;
    }

    /**
     * This setting identifies sparse index.
     */
    public static final Setting<Boolean> IS_SPARSE_INDEX_SETTING = Setting.boolSetting(SPARSE_INDEX, false, IndexScope, Final);

}

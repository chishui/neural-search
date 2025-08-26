/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import org.apache.lucene.util.Accountable;

public abstract class AccountableTracker implements Accountable {
    private final RamBytesRecorder recorder = new RamBytesRecorder(0);

    public void recordUsedBytes(long bytes) {
        recorder.record(bytes);
    }

    @Override
    public long ramBytesUsed() {
        return recorder.getBytes();
    }
}

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.io;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/**
 * Wraps a Lucene {@link IndexOutput} for JNI streaming writes.
 * <p>
 * The native side accumulates data in a native buffer, then on flush
 * calls {@link #writeBytes(byte[], int, int)} to write directly to the
 * underlying IndexOutput.
 */
public class IndexOutputWrapper implements Closeable {
    private final IndexOutput indexOutput;

    public IndexOutputWrapper(IndexOutput indexOutput) {
        this.indexOutput = indexOutput;
    }

    /**
     * Called from native code to write bytes directly to the IndexOutput.
     *
     * @param bytes  the byte array containing data
     * @param offset start offset in the array
     * @param length number of bytes to write
     */
    public void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        indexOutput.writeBytes(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        indexOutput.close();
    }
}

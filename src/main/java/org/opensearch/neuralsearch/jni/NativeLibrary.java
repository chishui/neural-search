/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.sparse.common.SparseQueryResult;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

@Log4j2
public class NativeLibrary {
    public static String LIBRARY_NAME = "opensearch_neuralsearch_nsparse";

    static {
        try {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                System.loadLibrary(LIBRARY_NAME);
                return null;
            });
        } catch (UnsatisfiedLinkError e) {
            log.error("Failed to load library: {}", LIBRARY_NAME);
            throw e;
        }
        log.info("Loaded library: {}", LIBRARY_NAME);
        initLibrary();
    }

    public static native void initLibrary();

    public static native long initIndex(long numDocs, int dim, Map<String, Object> parameters);

    public static native void insertToIndex(
        long indexAddress,
        int[] ids,
        long indicesAddress,
        long tokensAddress,
        long valueAddress,
        int threadCount
    );

    public static native void writeIndex(long indexAddress, IndexOutputWrapper output);

    public static native long loadIndex(String indexPath);

    public static native SparseQueryResult[] queryIndex(
        long indexPointer,
        int[] tokens,
        float[] weights,
        int k,
        Map<String, ?> methodParameters
    );

    public static native SparseQueryResult[] queryIndexWithFilter(
        long indexPointer,
        int[] tokens,
        float[] weights,
        int k,
        Map<String, ?> methodParameters,
        long[] filterIds,
        int filterIdsType
    );

    public static native void freeIndex(long indexAddress);

    // common functions
    public static native void transferVectors(long memoryAddresses[], int indices[], int tokens[], float weights[]);
}

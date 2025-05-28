/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.jni;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class NativeLibrary {
    static private NativeLibrary instance;

    public static NativeLibrary getInstance() {
        if (instance == null) {
            instance = new NativeLibrary();
        }
        return instance;
    }

    static {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            System.loadLibrary("opensearch_sparse_ann"); // without lib prefix and .so extension
            return null;
        });
    }

    public native float dp(short[] tokens1, float[] values1, float[] values2); // declare your native method
}

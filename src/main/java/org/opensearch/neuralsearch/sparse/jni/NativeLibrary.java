/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.jni;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.lang.invoke.MethodHandle;

public class NativeLibrary {
    static private NativeLibrary instance;
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LIBRARY_LOOKUP;
    private static final MethodHandle SPARSE_DOT_PRODUCT;
    private static final MethodHandle SPARSE_DOT_PRODUCT_INT8;

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
        // Alternative: load from specific path
        // LIBRARY_LOOKUP = SymbolLookup.libraryLookup(Path.of("path/to/libsparse_vector_ops.so"), Arena.global());

        LIBRARY_LOOKUP = SymbolLookup.loaderLookup();

        // Look up the native function
        MemorySegment sparseDoProductSymbol = LIBRARY_LOOKUP.find("sparse_dot_product_native")
            .orElseThrow(() -> new RuntimeException("sparse_dot_product_native function not found"));

        MemorySegment sparseDoProductInt8Symbol = LIBRARY_LOOKUP.find("sparse_dot_product_native_int8")
            .orElseThrow(() -> new RuntimeException("sparse_dot_product_native_int8 function not found"));

        // Define function descriptor: float sparse_dot_product_native(short* tokens, float* values1, float* values2, long v1_size, long
        // v2_size)
        FunctionDescriptor descriptor = FunctionDescriptor.of(
            ValueLayout.JAVA_FLOAT,    // return type: float
            ValueLayout.ADDRESS,       // short* tokens
            ValueLayout.ADDRESS,       // float* values1
            ValueLayout.ADDRESS,       // float* values2
            ValueLayout.JAVA_LONG,     // long v1_size
            ValueLayout.JAVA_LONG      // long v2_size
        );

        SPARSE_DOT_PRODUCT = LINKER.downcallHandle(sparseDoProductSymbol, descriptor);

        FunctionDescriptor descriptorInt8 = FunctionDescriptor.of(
            ValueLayout.JAVA_INT,    // return type: int
            ValueLayout.ADDRESS,       // short* tokens
            ValueLayout.ADDRESS,       // int8* values1
            ValueLayout.ADDRESS,       // int8* values2
            ValueLayout.JAVA_LONG,     // long v1_size
            ValueLayout.JAVA_LONG      // long v2_size
        );

        SPARSE_DOT_PRODUCT_INT8 = LINKER.downcallHandle(sparseDoProductInt8Symbol, descriptorInt8);

    }

    public native float dp(short[] tokens1, float[] values1, float[] values2); // declare your native method

    public native float dp2(); // declare your native method

    public native float dp3(ByteBuffer tokens1, ByteBuffer values1, ByteBuffer values2); // declare your native method

    public native float dp4(ByteBuffer tokens1);// declare your native method

    /**
     * Compute dot product between sparse vectors using Foreign Function API
     * @param tokens1 indices for sparse vector 1
     * @param values1 values for sparse vector 1
     * @param values2 dense vector 2 (indexed by tokens1)
     * @return dot product result
     */
    public static float sparseDotProduct(short[] tokens1, float[] values1, float[] values2) {
        try (Arena arena = Arena.ofConfined()) {
            // Allocate native memory and copy data
            MemorySegment tokensSegment = arena.allocate(tokens1.length * 2);
            MemorySegment values1Segment = arena.allocate(4 * values1.length);
            MemorySegment values2Segment = arena.allocate(4 * values2.length);

            // Copy data to native memory
            for (int i = 0; i < tokens1.length; i++) {
                tokensSegment.setAtIndex(ValueLayout.JAVA_SHORT, i, tokens1[i]);
            }
            for (int i = 0; i < values1.length; i++) {
                values1Segment.setAtIndex(ValueLayout.JAVA_FLOAT, i, values1[i]);
            }
            for (int i = 0; i < values2.length; i++) {
                values2Segment.setAtIndex(ValueLayout.JAVA_FLOAT, i, values2[i]);
            }

            // Call native function
            return (float) SPARSE_DOT_PRODUCT.invoke(
                tokensSegment,
                values1Segment,
                values2Segment,
                (long) tokens1.length,
                (long) values2.length
            );
        } catch (Throwable e) {
            throw new RuntimeException("Native sparse dot product failed", e);
        }
    }

    /**
     * Optimized version using bulk memory operations
     */
    public static float sparseDotProductOptimized(short[] tokens1, float[] values1, float[] values2) {
        try (Arena arena = Arena.ofConfined()) {
            // Allocate native memory
            MemorySegment tokensSegment = arena.allocate(2 * tokens1.length);
            MemorySegment values1Segment = arena.allocate(4 * values1.length);
            MemorySegment values2Segment = arena.allocate(4 * values2.length);

            // Bulk copy using MemorySegment.copy()
            MemorySegment.copy(tokens1, 0, tokensSegment, ValueLayout.JAVA_SHORT, 0, tokens1.length);
            MemorySegment.copy(values1, 0, values1Segment, ValueLayout.JAVA_FLOAT, 0, values1.length);
            MemorySegment.copy(values2, 0, values2Segment, ValueLayout.JAVA_FLOAT, 0, values2.length);

            // Call native function
            return (float) SPARSE_DOT_PRODUCT.invoke(
                tokensSegment,
                values1Segment,
                values2Segment,
                (long) tokens1.length,
                (long) values2.length
            );
        } catch (Throwable e) {
            throw new RuntimeException("Native sparse dot product failed", e);
        }
    }

    public static float sparseDotProductDirect(
        java.nio.ByteBuffer tokens1Buffer,
        java.nio.ByteBuffer values1Buffer,
        java.nio.ByteBuffer values2Buffer
    ) {
        try {
            // Convert ByteBuffers to MemorySegments (zero-copy for direct buffers)
            MemorySegment tokensSegment = MemorySegment.ofBuffer(tokens1Buffer);
            MemorySegment values1Segment = MemorySegment.ofBuffer(values1Buffer);
            MemorySegment values2Segment = MemorySegment.ofBuffer(values2Buffer);

            long v1Size = tokens1Buffer.capacity() / Short.BYTES;
            long v2Size = values2Buffer.capacity() / Float.BYTES;

            return (float) SPARSE_DOT_PRODUCT.invoke(tokensSegment, values1Segment, values2Segment, v1Size, v2Size);
        } catch (Throwable e) {
            throw new RuntimeException("Direct sparse dot product failed", e);
        }
    }

    public static float sparseDotProductDirectInt8(
        java.nio.ByteBuffer tokens1Buffer,
        java.nio.ByteBuffer values1Buffer,
        java.nio.ByteBuffer values2Buffer
    ) {
        try {
            // Convert ByteBuffers to MemorySegments (zero-copy for direct buffers)
            MemorySegment tokensSegment = MemorySegment.ofBuffer(tokens1Buffer);
            MemorySegment values1Segment = MemorySegment.ofBuffer(values1Buffer);
            MemorySegment values2Segment = MemorySegment.ofBuffer(values2Buffer);

            long v1Size = tokens1Buffer.capacity() / Short.BYTES;
            long v2Size = values2Buffer.capacity() / Byte.BYTES;

            return (float) SPARSE_DOT_PRODUCT_INT8.invoke(tokensSegment, values1Segment, values2Segment, v1Size, v2Size);
        } catch (Throwable e) {
            throw new RuntimeException("Direct sparse dot product failed", e);
        }
    }

    /**
     * High-performance version with persistent off-heap memory
     */
    public static class OffHeapSparseVector implements AutoCloseable {
        private final Arena arena;
        private final MemorySegment tokensSegment;
        private final MemorySegment valuesSegment;
        private final long size;

        public OffHeapSparseVector(short[] tokens, float[] values) {
            if (tokens.length != values.length) {
                throw new IllegalArgumentException("Tokens and values arrays must have same length");
            }

            this.arena = Arena.ofShared();
            this.size = tokens.length;

            // Allocate and copy data
            this.tokensSegment = arena.allocate(2 * tokens.length);
            this.valuesSegment = arena.allocate(4 * values.length);

            for (int i = 0; i < tokens.length; i++) {
                tokensSegment.setAtIndex(ValueLayout.JAVA_SHORT, i, tokens[i]);
                valuesSegment.setAtIndex(ValueLayout.JAVA_FLOAT, i, values[i]);
            }
        }

        public float dotProduct(OffHeapSparseVector other) {
            try {
                return (float) SPARSE_DOT_PRODUCT.invoke(
                    this.tokensSegment,
                    this.valuesSegment,
                    other.valuesSegment, // Assuming other is dense or compatible
                    this.size,
                    other.size
                );
            } catch (Throwable e) {
                throw new RuntimeException("Off-heap dot product failed", e);
            }
        }

        @Override
        public void close() {
            arena.close();
        }
    }
}

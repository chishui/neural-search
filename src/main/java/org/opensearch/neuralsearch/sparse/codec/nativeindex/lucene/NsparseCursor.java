/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex.lucene;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.Locale;

/**
 * Sequential reader over the nsparse payload of an engine file, with the alignment rules of
 * {@code nsparse/io/align.h}.
 *
 * <p>This is the Java counterpart of the C++ {@code MmapCursor}: the same walk over the same
 * layout, reading through an {@link IndexInput} instead of borrowing from a mapping. The padding a
 * serializer inserted before each array is computed from the absolute file position, exactly as the
 * mapped reader computes it from the mapping offset -- the payload starts at file offset 0, so the
 * two agree byte for byte.
 *
 * <p>Only the C++ reader needs an array to land on an address its element type can be loaded from;
 * a Java reader has no such constraint. The padding is still consumed, because skipping it is what
 * keeps every later offset in step.
 */
final class NsparseCursor {

    /** Past this a count is not a count, it is a corrupt file: nothing here is allocated per byte. */
    private static final long MAX_PLAUSIBLE_COUNT = Integer.MAX_VALUE;

    private final IndexInput in;
    /** Where the payload ends: the file length less its Lucene footer. Every read is bounded by it. */
    private final long payloadEnd;

    NsparseCursor(IndexInput in, long payloadEnd) {
        this.in = in;
        this.payloadEnd = payloadEnd;
    }

    long position() {
        return in.getFilePointer();
    }

    long payloadEnd() {
        return payloadEnd;
    }

    void seek(long position) throws IOException {
        checkBounds(position, 0, "seek");
        in.seek(position);
    }

    void skip(long bytes) throws IOException {
        if (bytes < 0) {
            throw corrupt("negative skip of " + bytes + " bytes");
        }
        checkBounds(position(), bytes, "skip");
        in.skipBytes(bytes);
    }

    /** Consumes the padding {@code io_align::pad_to} wrote at this point in the layout. */
    void skipPadding(long alignment) throws IOException {
        skip(NsparseFormat.paddingFor(position(), alignment));
    }

    int readInt() throws IOException {
        checkBounds(position(), Integer.BYTES, "int");
        return in.readInt();
    }

    long readLong() throws IOException {
        checkBounds(position(), Long.BYTES, "long");
        return in.readLong();
    }

    float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    int readByte() throws IOException {
        checkBounds(position(), 1, "byte");
        return in.readByte() & 0xFF;
    }

    /** A {@code uint16_t}, left in a short so a large array of them stays two bytes per entry. */
    short readShort() throws IOException {
        checkBounds(position(), Short.BYTES, "short");
        return in.readShort();
    }

    /**
     * A {@code size_t} count, rejected unless it is a plausible array length. Every caller uses the
     * result to size an allocation or to bound a walk, so a wrapped or garbage count has to fail
     * here rather than downstream.
     */
    int readCount(String what) throws IOException {
        long value = readLong();
        if (value < 0 || value > MAX_PLAUSIBLE_COUNT) {
            throw corrupt("implausible " + what + " count " + Long.toUnsignedString(value));
        }
        return (int) value;
    }

    /** A {@code u64} used as a length or an offset. */
    long readUnsignedLong(String what) throws IOException {
        long value = readLong();
        if (value < 0) {
            throw corrupt("implausible " + what + " " + Long.toUnsignedString(value));
        }
        return value;
    }

    void readBytes(byte[] dest, int offset, int length) throws IOException {
        checkBounds(position(), length, "bytes");
        in.readBytes(dest, offset, length);
    }

    /** An {@code int32_t}/{@code uint32_t} array, preceded by the padding {@code write_padded} wrote. */
    int[] readPaddedIntArray(int count) throws IOException {
        skipPadding(Integer.BYTES);
        checkBounds(position(), (long) count * Integer.BYTES, "int array");
        int[] values = new int[count];
        for (int i = 0; i < count; ++i) {
            values[i] = in.readInt();
        }
        return values;
    }

    /** A {@code uint16_t} array widened to int, so callers never sign-extend a term id. */
    int[] readPaddedUnsignedShortArray(int count) throws IOException {
        skipPadding(Short.BYTES);
        checkBounds(position(), (long) count * Short.BYTES, "short array");
        int[] values = new int[count];
        for (int i = 0; i < count; ++i) {
            values[i] = in.readShort() & 0xFFFF;
        }
        return values;
    }

    /**
     * A byte array whose padding is to {@code alignment} rather than to 1: values are bytes on the
     * wire but read as {@code alignment}-wide words, so that is what they were padded to.
     */
    byte[] readPaddedByteArray(int count, long alignment) throws IOException {
        skipPadding(alignment);
        byte[] values = new byte[count];
        readBytes(values, 0, count);
        return values;
    }

    /** Advances past an array without reading it, padding included. */
    void skipPaddedArray(int count, long elementBytes) throws IOException {
        skipPadding(elementBytes);
        skip((long) count * elementBytes);
    }

    private void checkBounds(long position, long length, String what) throws IOException {
        if (position < 0 || length < 0 || position + length > payloadEnd) {
            throw corrupt(
                String.format(
                    Locale.ROOT,
                    "read of %d bytes (%s) at offset %d runs past the payload end %d",
                    length,
                    what,
                    position,
                    payloadEnd
                )
            );
        }
    }

    private IOException corrupt(String message) {
        return new IOException("Corrupt native sparse index [" + in + "]: " + message);
    }
}

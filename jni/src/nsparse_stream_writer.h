/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

#ifndef NSPARSE_STREAM_WRITER_H
#define NSPARSE_STREAM_WRITER_H

#include <jni.h>

#include <cstddef>
#include <memory>
#include <vector>

#include "nsparse/io/io.h"

namespace neural_search_jni {

/**
 * Manages a native-side buffer and flushes to a Java IndexOutputWrapper.
 *
 * On flush, creates a temporary Java byte[], copies native data into it via
 * SetByteArrayRegion, then calls IndexOutputWrapper.writeBytes(byte[], int, int)
 * to write directly to Lucene's IndexOutput.
 */
class JniBufferedWriter {
public:
    JniBufferedWriter(JNIEnv* env, jobject output);

    /**
     * Append data to the native buffer, flushing to Java when full.
     */
    void write(const void* ptr, size_t bytes);

    /**
     * Flush any remaining buffered data to Java.
     */
    void flush();

private:
    void flushBuffer(size_t length);

    JNIEnv* env_;
    jobject output_;            // IndexOutputWrapper instance
    jmethodID write_method_;    // IndexOutputWrapper.writeBytes(byte[], int, int)
    size_t capacity_;           // Buffer size / flush threshold
    std::vector<char> buffer_;  // Pre-allocated native buffer
    size_t pos_;                // Current write position in buffer
};

/**
 * nsparse::IOWriter implementation that streams serialized index data
 * to a Java IndexOutputWrapper via JniBufferedWriter.
 */
class NsparseStreamWriter : public nsparse::IOWriter {
public:
    explicit NsparseStreamWriter(std::unique_ptr<JniBufferedWriter> writer);
    void write(void* ptr, size_t size, size_t nitems) override;
    void close() override;

private:
    std::unique_ptr<JniBufferedWriter> writer_;
};

}  // namespace neural_search_jni

#endif  // NSPARSE_STREAM_WRITER_H

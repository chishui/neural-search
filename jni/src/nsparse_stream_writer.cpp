/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

#include "nsparse_stream_writer.h"

#include <cstring>
#include <stdexcept>

namespace neural_search_jni {

// ============================================================================
// JniBufferedWriter
// ============================================================================

static constexpr size_t DEFAULT_BUFFER_SIZE = 64 * 1024;  // 64 KB

JniBufferedWriter::JniBufferedWriter(JNIEnv* env, jobject output)
    : env_(env),
      output_(output),
      capacity_(DEFAULT_BUFFER_SIZE),
      buffer_(DEFAULT_BUFFER_SIZE),
      pos_(0) {
    jclass cls = env_->GetObjectClass(output_);

    // Look up IndexOutputWrapper.writeBytes(byte[], int, int)
    write_method_ = env_->GetMethodID(cls, "writeBytes", "([BII)V");
    if (write_method_ == nullptr) {
        throw std::runtime_error(
            "IndexOutputWrapper.writeBytes(byte[], int, int) not found");
    }
}

void JniBufferedWriter::write(const void* ptr, size_t bytes) {
    auto src = static_cast<const char*>(ptr);
    size_t remaining = bytes;

    while (remaining > 0) {
        size_t space = capacity_ - pos_;
        size_t toCopy = std::min(remaining, space);

        std::memcpy(buffer_.data() + pos_, src, toCopy);
        pos_ += toCopy;
        src += toCopy;
        remaining -= toCopy;

        if (pos_ == capacity_) {
            flushBuffer(capacity_);
        }
    }
}

void JniBufferedWriter::flush() {
    if (pos_ > 0) {
        flushBuffer(pos_);
    }
}

void JniBufferedWriter::flushBuffer(size_t length) {
    // Create a temporary Java byte[], copy native data in, call writeBytes
    jbyteArray jbuf = env_->NewByteArray(static_cast<jint>(length));
    env_->SetByteArrayRegion(
        jbuf, 0, static_cast<jint>(length),
        reinterpret_cast<const jbyte*>(buffer_.data()));

    env_->CallVoidMethod(
        output_, write_method_, jbuf, 0, static_cast<jint>(length));

    env_->DeleteLocalRef(jbuf);

    if (env_->ExceptionCheck()) {
        throw std::runtime_error(
            "Exception in IndexOutputWrapper.writeBytes");
    }

    pos_ = 0;
}

// ============================================================================
// NsparseStreamWriter
// ============================================================================

NsparseStreamWriter::NsparseStreamWriter(
    std::unique_ptr<JniBufferedWriter> writer)
    : writer_(std::move(writer)) {}

void NsparseStreamWriter::write(void* ptr, size_t size, size_t nitems) {
    writer_->write(ptr, size * nitems);
}

void NsparseStreamWriter::close() {
    writer_->flush();
}

}  // namespace neural_search_jni

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <jni.h>
#include <cstdint>
#include "common.h"
#include "jni_util.h"

extern "C" {

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_JNICommons_freeVectorData(
    JNIEnv* env, jclass cls, jlong address) {
    try {
        if (address != 0) {
            auto* vec = reinterpret_cast<std::vector<int32_t>*>(address);
            delete vec;
        }
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

}  // extern "C"

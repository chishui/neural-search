/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include <jni.h>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>

#include "nsparse_wrapper.h"
#include "common.h"
#include "jni_util.h"

namespace {

/**
 * Convert a Java Map<String, Object> to a C++ std::map<std::string, jobject>.
 * Uses cached class/method IDs for performance.
 */
std::map<std::string, jobject> javaMapToStdMap(JNIEnv* env, jobject jmap) {
    std::map<std::string, jobject> result;
    if (jmap == nullptr) return result;

    auto& c = neural_search_jni::cachedRefs;

    jobject entrySet = env->CallObjectMethod(jmap, c.getMethod(neural_search_jni::MAP_ENTRY_SET));
    jobject iterator = env->CallObjectMethod(entrySet, c.getMethod(neural_search_jni::SET_ITERATOR));

    while (env->CallBooleanMethod(iterator, c.getMethod(neural_search_jni::ITERATOR_HAS_NEXT))) {
        jobject entry = env->CallObjectMethod(iterator, c.getMethod(neural_search_jni::ITERATOR_NEXT));
        auto jkey = static_cast<jstring>(env->CallObjectMethod(entry, c.getMethod(neural_search_jni::ENTRY_GET_KEY)));
        jobject jvalue = env->CallObjectMethod(entry, c.getMethod(neural_search_jni::ENTRY_GET_VALUE));

        const char* keyChars = env->GetStringUTFChars(jkey, nullptr);
        result[std::string(keyChars)] = jvalue;
        env->ReleaseStringUTFChars(jkey, keyChars);
        env->DeleteLocalRef(entry);
    }

    return result;
}

/**
 * Create a Java SparseQueryResult[] from C++ distance/label arrays.
 * Uses cached class ref and constructor ID.
 */
jobjectArray buildSparseQueryResults(JNIEnv* env, const float* distances,
                                     const int32_t* labels, int k) {
    auto& c = neural_search_jni::cachedRefs;
    jclass resultClass = c.getClass(neural_search_jni::SPARSE_QUERY_RESULT);
    jmethodID ctor = c.getMethod(neural_search_jni::SPARSE_QUERY_RESULT_CTOR);

    int validCount = 0;
    for (int i = 0; i < k; i++) {
        if (labels[i] != -1) validCount++;
    }

    jobjectArray results = env->NewObjectArray(validCount, resultClass, nullptr);
    int idx = 0;
    for (int i = 0; i < k; i++) {
        if (labels[i] == -1) continue;
        jobject obj = env->NewObject(resultClass, ctor,
                                     static_cast<jint>(labels[i]),
                                     static_cast<jfloat>(distances[i]));
        env->SetObjectArrayElement(results, idx++, obj);
        env->DeleteLocalRef(obj);
    }
    return results;
}

}  // anonymous namespace

// ============================================================================
// JNI_OnLoad — cache class refs and method IDs once at library load time.
// ============================================================================

extern "C" JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        return JNI_ERR;
    }
    try {
        neural_search_jni::cachedRefs.init(env);
    } catch (const std::exception& e) {
        neural_search_jni::ThrowJavaException(env, "java/lang/RuntimeException", e.what());
        return JNI_ERR;
    }
    return JNI_VERSION_1_8;
}

extern "C" JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env = nullptr;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) == JNI_OK) {
        neural_search_jni::cachedRefs.release(env);
    }
}

// ============================================================================
// JNI exported functions for org.opensearch.neuralsearch.jni.NativeLibrary
// ============================================================================

extern "C" {

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_initLibrary(
    JNIEnv* env, jclass cls) {
    try {
        neural_search_jni::nsparse_wrapper::initLibrary();
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT jlong JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_initIndex(
    JNIEnv* env, jclass cls, jlong numDocs, jint dim, jobject parameters) {
    try {
        auto params = javaMapToStdMap(env, parameters);
        return neural_search_jni::nsparse_wrapper::initIndex(numDocs, dim, params, env);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return 0;
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_insertToIndex(
    JNIEnv* env, jclass cls, jlong indexAddress, jintArray ids,
    jlong indicesAddress, jlong tokensAddress, jlong valueAddress,
    jint threadCount) {
    try {
        jint* idElements = env->GetIntArrayElements(ids, nullptr);
        jint numIds = env->GetArrayLength(ids);

        neural_search_jni::nsparse_wrapper::insertToIndex(
            indexAddress,
            reinterpret_cast<const int32_t*>(idElements),
            numIds,
            indicesAddress, tokensAddress, valueAddress,
            threadCount);

        env->ReleaseIntArrayElements(ids, idElements, JNI_ABORT);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_writeIndex(
    JNIEnv* env, jclass cls, jlong indexAddress, jobject output) {
    try {
        neural_search_jni::nsparse_wrapper::writeIndex(indexAddress, output, env);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT jlong JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_loadIndex(
    JNIEnv* env, jclass cls, jstring indexPath) {
    try {
        const char* pathChars = env->GetStringUTFChars(indexPath, nullptr);
        std::string path(pathChars);
        env->ReleaseStringUTFChars(indexPath, pathChars);
        return neural_search_jni::nsparse_wrapper::loadIndex(path);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return 0;
    }
}

JNIEXPORT jobjectArray JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndex(
    JNIEnv* env, jclass cls, jlong indexPointer, jintArray jtokens,
    jfloatArray jweights, jint k, jobject methodParameters) {
    try {
        jint numTokens = env->GetArrayLength(jtokens);
        jint* tokenElements = env->GetIntArrayElements(jtokens, nullptr);
        jfloat* weightElements = env->GetFloatArrayElements(jweights, nullptr);

        std::vector<float> distances(k);
        std::vector<int32_t> labels(k);

        auto params = javaMapToStdMap(env, methodParameters);
        neural_search_jni::nsparse_wrapper::queryIndex(
            indexPointer,
            reinterpret_cast<const int32_t*>(tokenElements),
            weightElements,
            numTokens, k, params, env,
            distances.data(), labels.data());

        env->ReleaseIntArrayElements(jtokens, tokenElements, JNI_ABORT);
        env->ReleaseFloatArrayElements(jweights, weightElements, JNI_ABORT);

        return buildSparseQueryResults(env, distances.data(), labels.data(), k);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return nullptr;
    }
}

JNIEXPORT jobjectArray JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_queryIndexWithFilter(
    JNIEnv* env, jclass cls, jlong indexPointer, jintArray jtokens,
    jfloatArray jweights, jint k, jobject methodParameters,
    jlongArray jfilterIds, jint filterIdsType) {
    try {
        jint numTokens = env->GetArrayLength(jtokens);
        jint* tokenElements = env->GetIntArrayElements(jtokens, nullptr);
        jfloat* weightElements = env->GetFloatArrayElements(jweights, nullptr);
        jint numFilterIds = env->GetArrayLength(jfilterIds);
        jlong* filterIdElements = env->GetLongArrayElements(jfilterIds, nullptr);

        std::vector<float> distances(k);
        std::vector<int32_t> labels(k);

        auto params = javaMapToStdMap(env, methodParameters);
        // On some platforms jlong (long) != int64_t (long long), so copy
        std::vector<int64_t> filterIds64(filterIdElements, filterIdElements + numFilterIds);
        neural_search_jni::nsparse_wrapper::queryIndexWithFilter(
            indexPointer,
            reinterpret_cast<const int32_t*>(tokenElements),
            weightElements,
            numTokens, k, params,
            filterIds64.data(), numFilterIds, filterIdsType,
            env, distances.data(), labels.data());

        env->ReleaseIntArrayElements(jtokens, tokenElements, JNI_ABORT);
        env->ReleaseFloatArrayElements(jweights, weightElements, JNI_ABORT);
        env->ReleaseLongArrayElements(jfilterIds, filterIdElements, JNI_ABORT);

        return buildSparseQueryResults(env, distances.data(), labels.data(), k);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
        return nullptr;
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_freeIndex(
    JNIEnv* env, jclass cls, jlong indexAddress) {
    try {
        neural_search_jni::nsparse_wrapper::freeIndex(indexAddress);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

JNIEXPORT void JNICALL
Java_org_opensearch_neuralsearch_jni_NativeLibrary_transferVectors(
    JNIEnv* env, jclass cls, jlongArray jmemoryAddresses,
    jintArray jindices, jintArray jtokens, jfloatArray jweights) {
    try {
        jlong* memAddrs = env->GetLongArrayElements(jmemoryAddresses, nullptr);
        jint* indices = env->GetIntArrayElements(jindices, nullptr);
        jint indicesLen = env->GetArrayLength(jindices);
        jint* tokens = env->GetIntArrayElements(jtokens, nullptr);
        jint tokensLen = env->GetArrayLength(jtokens);
        jfloat* weights = env->GetFloatArrayElements(jweights, nullptr);
        jint weightsLen = env->GetArrayLength(jweights);

        neural_search_jni::transferVectors(
            reinterpret_cast<int64_t*>(memAddrs),
            reinterpret_cast<const int32_t*>(indices), indicesLen,
            reinterpret_cast<const int32_t*>(tokens), tokensLen,
            weights, weightsLen);

        env->ReleaseLongArrayElements(jmemoryAddresses, memAddrs, 0);
        env->ReleaseIntArrayElements(jindices, indices, JNI_ABORT);
        env->ReleaseIntArrayElements(jtokens, tokens, JNI_ABORT);
        env->ReleaseFloatArrayElements(jweights, weights, JNI_ABORT);
    } catch (...) {
        neural_search_jni::CatchCppExceptionAndThrowJava(env);
    }
}

}  // extern "C"

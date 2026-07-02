/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef __OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__
#define __OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__

#include <jni.h>
#include <string>
#include <unordered_map>

namespace neural_search_jni {

// ============================================================================
// Cached class key constants
// ============================================================================
inline const char* JAVA_MAP              = "java/util/Map";
inline const char* JAVA_SET              = "java/util/Set";
inline const char* JAVA_ITERATOR         = "java/util/Iterator";
inline const char* JAVA_MAP_ENTRY        = "java/util/Map$Entry";
inline const char* JAVA_NUMBER           = "java/lang/Number";
inline const char* JAVA_BOOLEAN          = "java/lang/Boolean";
inline const char* SPARSE_QUERY_RESULT   = "org/opensearch/neuralsearch/sparse/common/SparseQueryResult";
inline const char* JAVA_OOM_ERROR        = "java/lang/OutOfMemoryError";
inline const char* JAVA_ILLEGAL_ARGUMENT = "java/lang/IllegalArgumentException";
inline const char* JAVA_EXCEPTION        = "java/lang/Exception";
inline const char* JAVA_RUNTIME_EXCEPTION = "java/lang/RuntimeException";

// ============================================================================
// Cached method key constants
// ============================================================================
inline const char* MAP_ENTRY_SET         = "MAP_ENTRY_SET";
inline const char* SET_ITERATOR          = "SET_ITERATOR";
inline const char* ITERATOR_HAS_NEXT     = "ITERATOR_HAS_NEXT";
inline const char* ITERATOR_NEXT         = "ITERATOR_NEXT";
inline const char* ENTRY_GET_KEY         = "ENTRY_GET_KEY";
inline const char* ENTRY_GET_VALUE       = "ENTRY_GET_VALUE";
inline const char* NUMBER_FLOAT_VALUE    = "NUMBER_FLOAT_VALUE";
inline const char* NUMBER_INT_VALUE      = "NUMBER_INT_VALUE";
inline const char* BOOLEAN_VALUE         = "BOOLEAN_VALUE";
inline const char* SPARSE_QUERY_RESULT_CTOR = "SPARSE_QUERY_RESULT_CTOR";

// ============================================================================
// JniCachedRefs — map-based cache for class refs and method IDs
// ============================================================================

struct JniCachedRefs {
    std::unordered_map<std::string, jclass>    cachedClasses;
    std::unordered_map<std::string, jmethodID> cachedMethods;

    jclass getClass(const char* key) const;
    jmethodID getMethod(const char* key) const;
    void init(JNIEnv* env);
    void release(JNIEnv* env);

private:
    void cacheClass(JNIEnv* env, const char* className);
    void cacheMethod(JNIEnv* env, const char* methodKey,
                     const char* classKey, const char* methodName,
                     const char* sig);
};

// ============================================================================
// Exception helpers
// ============================================================================

void ThrowJavaException(JNIEnv* env, const char* classKey, const char* message);
void CatchCppExceptionAndThrowJava(JNIEnv* env);

// ============================================================================
// Global singleton (defined in jni_util.cpp)
// ============================================================================
extern JniCachedRefs cachedRefs;

}  // namespace neural_search_jni

#endif //__OPENSEARCH_NEURALSEARCH_JNI_UTIL_H__

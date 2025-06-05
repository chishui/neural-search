#include "org_opensearch_neuralsearch_sparse_jni_NativeLibrary.h"
#include <iostream>
#include <chrono>

using namespace std;

JNIEXPORT jfloat JNICALL Java_org_opensearch_neuralsearch_sparse_jni_NativeLibrary_dp
  (JNIEnv *env, jobject obj, jshortArray tokens1, jfloatArray values1,
   jfloatArray values2) {

    // Get arrays elements
    jshort* t1 = env->GetShortArrayElements(tokens1, nullptr);
    jfloat* v1 = env->GetFloatArrayElements(values1, nullptr);
    jfloat* v2 = env->GetFloatArrayElements(values2, nullptr);

    // Get array lengths
    jsize v1_size = env->GetArrayLength(tokens1);
    jsize v2_size = env->GetArrayLength(values2);

    typedef float RET_T;
    RET_T result = 0;
    // Early exit for empty vectors
    if (v1_size == 0 || v2_size == 0) return 0;

    // Loop unrolling for better performance
    const size_t unroll_factor = 4;
    const size_t limit = v1_size - (v1_size % unroll_factor);
    size_t i = 0;

    // Main loop with unrolling
    for (; i < limit; i += unroll_factor) {
        if (t1[i] >= v2_size) break;
        result += RET_T(v1[i]) * v2[t1[i]];

        if (t1[i + 1] >= v2_size) {
            ++i;
            break;
        }
        result += RET_T(v1[i + 1]) * v2[t1[i + 1]];

        if (t1[i + 2] >= v2_size) {
            i += 2;
            break;
        }
        result += RET_T(v1[i + 2]) * v2[t1[i + 2]];

        if (t1[i + 3] >= v2_size) {
            i += 3;
            break;
        }
        result += RET_T(v1[i + 3]) * v2[t1[i + 3]];
    }

    // Handle remaining elements
    for (; i < v1_size; ++i) {
        if (t1[i] >= v2_size) break;
        result += RET_T(v1[i]) * v2[t1[i]];
    }


    // Release arrays
    env->ReleaseShortArrayElements(tokens1, t1, JNI_ABORT);
    env->ReleaseFloatArrayElements(values1, v1, JNI_ABORT);
    env->ReleaseFloatArrayElements(values2, v2, JNI_ABORT);

    return result;
}

JNIEXPORT jfloat JNICALL Java_org_opensearch_neuralsearch_sparse_jni_NativeLibrary_dp2
  (JNIEnv *, jobject) {
    volatile float result = 0;
    for (int i = 0; i < 100; ++i) {
        result += 1;
    }
    return result;
  }

 JNIEXPORT jfloat JNICALL Java_org_opensearch_neuralsearch_sparse_jni_NativeLibrary_dp3
   (JNIEnv *env, jobject obj, jobject tokens1, jobject values1, jobject values2)
   {
    short* t1 = (short*)env->GetDirectBufferAddress(tokens1);
    float* v1 = (float*)env->GetDirectBufferAddress(values1);
    float* v2 = (float*)env->GetDirectBufferAddress(values2);

    // Get buffer capacity
   jlong v1_size = env->GetDirectBufferCapacity(tokens1) / sizeof(short);  // Changed
   jlong v2_size = env->GetDirectBufferCapacity(values2) / sizeof(float);  // Changed
    typedef float RET_T;
    RET_T result = 0;
    // Early exit for empty vectors
    if (v1_size == 0 || v2_size == 0) return 0;

    // Loop unrolling for better performance
    const size_t unroll_factor = 4;
    const size_t limit = v1_size - (v1_size % unroll_factor);
    size_t i = 0;

    // Main loop with unrolling
    for (; i < limit; i += unroll_factor) {
        if (t1[i] >= v2_size) break;
        result += RET_T(v1[i]) * v2[t1[i]];

        if (t1[i + 1] >= v2_size) {
            ++i;
            break;
        }
        result += RET_T(v1[i + 1]) * v2[t1[i + 1]];

        if (t1[i + 2] >= v2_size) {
            i += 2;
            break;
        }
        result += RET_T(v1[i + 2]) * v2[t1[i + 2]];

        if (t1[i + 3] >= v2_size) {
            i += 3;
            break;
        }
        result += RET_T(v1[i + 3]) * v2[t1[i + 3]];
    }

    // Handle remaining elements
    for (; i < v1_size; ++i) {
        if (t1[i] >= v2_size) break;
        result += RET_T(v1[i]) * v2[t1[i]];
    }
    return result;
   }

  JNIEXPORT jfloat JNICALL Java_org_opensearch_neuralsearch_sparse_jni_NativeLibrary_dp4(JNIEnv *env, jobject obj, jobject value) {
       float* v = (float*)env->GetDirectBufferAddress(value);
      jlong size = env->GetDirectBufferCapacity(value) / sizeof(float);  // Changed
        std::cout<<"size:"<<size<<" v[0]:"<<v[0]<<" v[1]:"<<v[1]<<std::endl;
        return 0;
  }

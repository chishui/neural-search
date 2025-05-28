#include "org_opensearch_neuralsearch_sparse_common_NativeLibrary.h"

JNIEXPORT jfloat JNICALL Java_org_opensearch_neuralsearch_sparse_common_NativeLibrary_dp
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

#include <cstdint>
#include <cstddef>
#include <iostream>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Optimized sparse dot product implementation
 * @param tokens indices for sparse vector 1
 * @param values1 values for sparse vector 1
 * @param values2 dense vector 2 (indexed by tokens)
 * @param v1_size length of sparse vector 1
 * @param v2_size length of dense vector 2
 * @return dot product result
 */
__attribute__((visibility("default"))) float sparse_dot_product_native(const int16_t* tokens,
                               const float* values1,
                               const float* values2,
                               int64_t v1_size,
                               int64_t v2_size) {

    float result = 0.0f;

    // Early exit for empty vectors
    if (v1_size == 0 || v2_size == 0) {
        return 0.0f;
    }

    // Loop unrolling for better performance
    const size_t unroll_factor = 4;
    const size_t limit = v1_size - (v1_size % unroll_factor);
    size_t i = 0;

    // Main loop with unrolling
    for (; i < limit; i += unroll_factor) {
        // First element
        if (tokens[i] >= v2_size) break;
        result += values1[i] * values2[tokens[i]];

        // Second element
        if (tokens[i + 1] >= v2_size) {
            ++i;
            break;
        }
        result += values1[i + 1] * values2[tokens[i + 1]];

        // Third element
        if (tokens[i + 2] >= v2_size) {
            i += 2;
            break;
        }
        result += values1[i + 2] * values2[tokens[i + 2]];

        // Fourth element
        if (tokens[i + 3] >= v2_size) {
            i += 3;
            break;
        }
        result += values1[i + 3] * values2[tokens[i + 3]];
    }

    // Handle remaining elements
    for (; i < v1_size; ++i) {
        if (tokens[i] >= v2_size) break;
        result += values1[i] * values2[tokens[i]];
    }

    return result;
}

__attribute__((visibility("default"))) int sparse_dot_product_native_int8(const int16_t* tokens,
                               const int8_t* values1,
                               const int8_t* values2,
                               int64_t v1_size,
                               int64_t v2_size) {

    int result = 0;
    // Early exit for empty vectors
    if (v1_size == 0 || v2_size == 0) {
        return 0;
    }

    // Loop unrolling for better performance
    const size_t unroll_factor = 4;
    const size_t limit = v1_size - (v1_size % unroll_factor);
    size_t i = 0;

    // Main loop with unrolling
    for (; i < limit; i += unroll_factor) {
        // First element
        if (tokens[i] >= v2_size) break;
        result += values1[i] * values2[tokens[i]];

        // Second element
        if (tokens[i + 1] >= v2_size) {
            ++i;
            break;
        }
        result += values1[i + 1] * values2[tokens[i + 1]];

        // Third element
        if (tokens[i + 2] >= v2_size) {
            i += 2;
            break;
        }
        result += values1[i + 2] * values2[tokens[i + 2]];

        // Fourth element
        if (tokens[i + 3] >= v2_size) {
            i += 3;
            break;
        }
        result += values1[i + 3] * values2[tokens[i + 3]];
    }

    // Handle remaining elements
    for (; i < v1_size; ++i) {
        if (tokens[i] >= v2_size) break;
        result += values1[i] * values2[tokens[i]];
    }

    return result;
}

/**
 * SIMD-optimized version using AVX2 (compile with -mavx2)
 */
#ifdef __AVX2__
#include <immintrin.h>

__attribute__((visibility("default"))) float sparse_dot_product_simd(const int16_t* tokens,
                             const float* values1,
                             const float* values2,
                             int64_t v1_size,
                             int64_t v2_size) {

    if (v1_size == 0 || v2_size == 0) return 0.0f;

    __m256 sum = _mm256_setzero_ps();
    const size_t simd_width = 8;
    const size_t simd_limit = v1_size - (v1_size % simd_width);

    size_t i = 0;

    // SIMD loop processing 8 elements at a time
    for (; i < simd_limit; i += simd_width) {
        // Load 8 values from values1
        __m256 vals1 = _mm256_loadu_ps(&values1[i]);

        // Gather 8 values from values2 using tokens as indices
        // Note: This is simplified - real implementation needs bounds checking
        __m256 vals2;
        float temp[8];
        for (int j = 0; j < 8; j++) {
            if (tokens[i + j] < v2_size) {
                temp[j] = values2[tokens[i + j]];
            } else {
                temp[j] = 0.0f;
                // Could break here for early exit
            }
        }
        vals2 = _mm256_loadu_ps(temp);

        // Multiply and accumulate
        sum = _mm256_fmadd_ps(vals1, vals2, sum);
    }

    // Horizontal add to get final sum
    float result[8];
    _mm256_storeu_ps(result, sum);
    float final_sum = result[0] + result[1] + result[2] + result[3] +
                     result[4] + result[5] + result[6] + result[7];

    // Handle remaining elements
    for (; i < v1_size; ++i) {
        if (tokens[i] >= v2_size) break;
        final_sum += values1[i] * values2[tokens[i]];
    }

    return final_sum;
}
#endif

#ifdef __cplusplus
}
#endif

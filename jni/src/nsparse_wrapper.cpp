/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

#include "nsparse_wrapper.h"

#include <jni.h>
#include <omp.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "jni_util.h"
#include "nsparse/id_selector.h"
#include "nsparse/index.h"
#include "nsparse/index_factory.h"
#include "nsparse/io/index_io.h"
#include "nsparse/seismic_index.h"
#include "nsparse/seismic_scalar_quantized_index.h"
#include "nsparse/types.h"
#include "nsparse_stream_writer.h"

namespace neural_search_jni::nsparse_wrapper {

namespace {

/**
 * Extract a std::string from a Java Map<String, Object> entry value.
 * Assumes the value is a java.lang.String.
 */
std::string jstring_to_string(JNIEnv* env, jobject jstr) {
    auto str = static_cast<jstring>(jstr);
    const char* chars = env->GetStringUTFChars(str, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(str, chars);
    return result;
}

/**
 * Extract a float from a Java Number object.
 */
float jobject_to_float(JNIEnv* env, jobject obj) {
    return env->CallFloatMethod(obj,
                                neural_search_jni::cachedRefs.getMethod(
                                    neural_search_jni::NUMBER_FLOAT_VALUE));
}

/**
 * Extract an int from a Java Number object.
 */
int jobject_to_int(JNIEnv* env, jobject obj) {
    return env->CallIntMethod(obj, neural_search_jni::cachedRefs.getMethod(
                                       neural_search_jni::NUMBER_INT_VALUE));
}

/**
 * Extract a bool from a Java Boolean object.
 */
bool jobject_to_bool(JNIEnv* env, jobject obj) {
    return env->CallBooleanMethod(obj, neural_search_jni::cachedRefs.getMethod(
                                           neural_search_jni::BOOLEAN_VALUE));
}

/**
 * Build the index_factory description string from Java parameters map.
 *
 * Expected keys:
 *   "idmap"     -> Boolean (if true, wraps with IDMapIndex)
 *   "index"     -> String  (e.g. "seismic_sq", "seismic", "brutal")
 *   "quantizer" -> String  (e.g. "8bit", "16bit")
 *   "vmin"      -> Number  (float)
 *   "vmax"      -> Number  (float)
 *   "lambda"    -> Number  (int)
 *   "beta"      -> Number  (float -> int)
 *   "alpha"     -> Number  (float)
 *
 * Produces e.g.:
 * "idmap,seismic_sq,quantizer=8bit|vmin=0.0|vmax=1.0|lambda=10|beta=5|alpha=0.4"
 */
std::string buildDescription(const std::map<std::string, jobject>& params,
                             JNIEnv* env) {
    std::string desc;

    // Check for idmap wrapper
    auto it = params.find("idmap");
    if (it != params.end() && jobject_to_bool(env, it->second)) {
        desc += "idmap,";
    }

    // Index type
    it = params.find("index");
    if (it == params.end()) {
        throw std::invalid_argument("Missing required parameter 'index'");
    }
    desc += jstring_to_string(env, it->second);

    // Collect construction parameters
    std::string paramStr;
    auto appendParam = [&](const std::string& key, const std::string& value) {
        if (!paramStr.empty()) paramStr += "|";
        paramStr += key + "=" + value;
    };

    it = params.find("quantizer");
    if (it != params.end()) {
        appendParam("quantizer", jstring_to_string(env, it->second));
    }
    it = params.find("vmin");
    if (it != params.end()) {
        appendParam("vmin", std::to_string(jobject_to_float(env, it->second)));
    }
    it = params.find("vmax");
    if (it != params.end()) {
        appendParam("vmax", std::to_string(jobject_to_float(env, it->second)));
    }
    it = params.find("lambda");
    if (it != params.end()) {
        appendParam("lambda", std::to_string(jobject_to_int(env, it->second)));
    }
    it = params.find("beta");
    if (it != params.end()) {
        // beta comes as float from Java (clusterRatio * nPostings) but
        // index_factory expects int
        appendParam("beta", std::to_string(static_cast<int>(
                                jobject_to_float(env, it->second))));
    }
    it = params.find("alpha");
    if (it != params.end()) {
        appendParam("alpha", std::to_string(jobject_to_float(env, it->second)));
    }

    if (!paramStr.empty()) {
        desc += "," + paramStr;
    }

    return desc;
}

/**
 * Build SeismicSearchParameters or SeismicSQSearchParameters from Java map.
 * Returns a heap-allocated SearchParameters (caller owns).
 */
nsparse::SearchParameters* buildSearchParameters(
    const std::map<std::string, jobject>& params, JNIEnv* env) {
    int cut = 10;
    float heapFactor = 1.0f;

    auto it = params.find("cut");
    if (it != params.end()) {
        cut = jobject_to_int(env, it->second);
    }
    it = params.find("heap_factor");
    if (it != params.end()) {
        heapFactor = jobject_to_float(env, it->second);
    }

    // If vmin/vmax are present, this is a SeismicSQ query
    auto vminIt = params.find("vmin");
    auto vmaxIt = params.find("vmax");
    if (vminIt != params.end() && vmaxIt != params.end()) {
        float vmin = jobject_to_float(env, vminIt->second);
        float vmax = jobject_to_float(env, vmaxIt->second);
        return new nsparse::SeismicSQSearchParameters(vmin, vmax, cut,
                                                      heapFactor);
    }

    return new nsparse::SeismicSearchParameters(cut, heapFactor);
}

}  // anonymous namespace

// ============================================================================
// NsparseWrapper method implementations
// ============================================================================

void initLibrary() {
    // No-op for now. Reserved for future global initialization.
}

int64_t initIndex(int64_t numDocs, int dim,
                  const std::map<std::string, jobject>& parameters,
                  JNIEnv* env) {
    std::string description = buildDescription(parameters, env);
    nsparse::Index* index = nsparse::index_factory(dim, description.c_str());
    return reinterpret_cast<int64_t>(index);
}

void insertToIndex(int64_t indexAddress, const int32_t* ids, int numIds,
                   int64_t indicesAddress, int64_t tokensAddress,
                   int64_t valuesAddress, int threadCount) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);
    auto* indptr = reinterpret_cast<std::vector<int32_t>*>(indicesAddress);
    auto* tokens = reinterpret_cast<std::vector<int32_t>*>(tokensAddress);
    auto* values = reinterpret_cast<std::vector<float>*>(valuesAddress);

    // nsparse uses uint16_t (term_t) for token indices — convert from int32_t
    std::vector<nsparse::term_t> termTokens(tokens->begin(), tokens->end());

    omp_set_num_threads(threadCount);
    index->add_with_ids(static_cast<nsparse::idx_t>(numIds),
                        reinterpret_cast<const nsparse::idx_t*>(indptr->data()),
                        termTokens.data(), values->data(),
                        reinterpret_cast<const nsparse::idx_t*>(ids));

    index->build();

    // Free the off-heap vectors now that they've been consumed
    delete indptr;
    delete tokens;
    delete values;
}

/**
 * Serialize the index to the given Java IndexOutputWrapper, then free the
 * index. Takes ownership of the index at indexAddress — the caller must not use
 * or free this address after writeIndex returns.
 */
void writeIndex(int64_t indexAddress, jobject output, JNIEnv* env) {
    std::unique_ptr<nsparse::Index> index(
        reinterpret_cast<nsparse::Index*>(indexAddress));
    auto jniWriter =
        std::make_unique<neural_search_jni::JniBufferedWriter>(env, output);
    neural_search_jni::NsparseStreamWriter streamWriter(std::move(jniWriter));
    nsparse::write_index(index.get(), &streamWriter);
    streamWriter.close();
}

int64_t loadIndex(const std::string& indexPath) {
    // read_index takes char* (non-const in the nsparse API)
    std::string path = indexPath;
    nsparse::Index* index = nsparse::read_index(path.data());
    return reinterpret_cast<int64_t>(index);
}

void queryIndex(int64_t indexAddress, const int32_t* tokens,
                const float* weights, int numTokens, int k,
                const std::map<std::string, jobject>& methodParameters,
                JNIEnv* env, float* distances, int32_t* labels) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);

    // Convert int32_t tokens to term_t (uint16_t)
    std::vector<nsparse::term_t> termTokens(tokens, tokens + numTokens);

    // Build single-query CSR indptr: [0, numTokens]
    nsparse::idx_t indptr[2] = {0, static_cast<nsparse::idx_t>(numTokens)};

    std::unique_ptr<nsparse::SearchParameters> params(
        methodParameters.empty()
            ? nullptr
            : buildSearchParameters(methodParameters, env));
    omp_set_num_threads(1);
    index->search(1, indptr, termTokens.data(), weights, k, distances,
                  reinterpret_cast<nsparse::idx_t*>(labels), params.get());
}

void queryIndexWithFilter(
    int64_t indexAddress, const int32_t* tokens, const float* weights,
    int numTokens, int k,
    const std::map<std::string, jobject>& methodParameters,
    const int64_t* filterIds, int numFilterIds, int filterIdsType, JNIEnv* env,
    float* distances, int32_t* labels) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);

    // Convert int32_t tokens to term_t (uint16_t)
    std::vector<nsparse::term_t> termTokens(tokens, tokens + numTokens);

    // Build single-query CSR indptr: [0, numTokens]
    nsparse::idx_t indptr[2] = {0, static_cast<nsparse::idx_t>(numTokens)};

    // Convert int64_t filter IDs to idx_t (int32_t)
    std::vector<nsparse::idx_t> idxFilterIds(filterIds,
                                             filterIds + numFilterIds);

    // Build the appropriate IDSelector based on filterIdsType
    std::unique_ptr<nsparse::IDSelector> idSelector;
    if (filterIdsType == 0) {
        idSelector = std::make_unique<nsparse::SetIDSelector>(
            idxFilterIds.size(), idxFilterIds.data());
    } else {
        idSelector = std::make_unique<nsparse::ArrayIDSelector>(
            idxFilterIds.size(), idxFilterIds.data());
    }

    std::unique_ptr<nsparse::SearchParameters> params(
        methodParameters.empty()
            ? nullptr
            : buildSearchParameters(methodParameters, env));
    if (!params) {
        params.reset(new nsparse::SeismicSearchParameters());
    }
    params->set_id_selector(idSelector.get());

    omp_set_num_threads(1);
    index->search(1, indptr, termTokens.data(), weights, k, distances,
                  reinterpret_cast<nsparse::idx_t*>(labels), params.get());
}

void freeIndex(int64_t indexAddress) {
    auto* index = reinterpret_cast<nsparse::Index*>(indexAddress);
    delete index;
}

}  // namespace neural_search_jni::nsparse_wrapper

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

# Check if neural-sparse-cpp exists
find_path(NSPARSE_REPO_DIR NAMES nsparse PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp NO_DEFAULT_PATH)

# If not, pull the updated submodule
if (NOT EXISTS ${NSPARSE_REPO_DIR})
    message(STATUS "Could not find neural-sparse-cpp. Pulling updated submodule.")
    execute_process(COMMAND git submodule update --init -- external/neural-sparse-cpp WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

if (APPLE)
    message(STATUS "darwin macos detected for nsparse")
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
        message(STATUS "detected Mac with ARM architecture for nsparse")
        if(CMAKE_C_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp")
            set(OpenMP_C_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)
        endif()

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/opt/libomp/include")
            set(OpenMP_CXX_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /opt/homebrew/opt/libomp/lib/libomp.dylib)
        endif()
    else()
        message(STATUS "detected Mac with x86 architecture for nsparse")
        if(CMAKE_C_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp")
            set(OpenMP_C_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /usr/local/opt/libomp/lib/libomp.dylib)
        endif()

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang\$")
            set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/usr/local/opt/libomp/include")
            set(OpenMP_CXX_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY /usr/local/opt/libomp/lib/libomp.dylib)
        endif()
    endif()
endif()

# Set relevant properties
set(NSPARSE_ENABLE_PYTHON OFF)
set(NSPARSE_ENABLE_TESTS OFF)
set(NSPARSE_ENABLE_BENCHMARKS OFF)

if(NOT DEFINED AVX2_ENABLED)
    set(AVX2_ENABLED true)
endif()

if(NOT DEFINED AVX512_ENABLED)
    set(AVX512_ENABLED true)
endif()

if(NOT DEFINED SVE_ENABLED)
    set(SVE_ENABLED true)
endif()

# Determine optimization level and target library
if(${CMAKE_SYSTEM_NAME} STREQUAL Windows OR ( NOT AVX2_ENABLED AND NOT AVX512_ENABLED))
    set(NSPARSE_OPT_LEVEL generic)
    set(TARGET_LINK_NSPARSE_LIB nsparse)
elseif(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64" OR ${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm64")
    if(APPLE)
        # Apple Silicon does not support SVE
        set(NSPARSE_OPT_LEVEL generic)
        set(TARGET_LINK_NSPARSE_LIB nsparse)
    elseif(SVE_ENABLED)
        set(NSPARSE_OPT_LEVEL sve)
        set(TARGET_LINK_NSPARSE_LIB nsparse_sve)
        string(PREPEND LIB_EXT "_sve")
    else()
        set(NSPARSE_OPT_LEVEL generic)
        set(TARGET_LINK_NSPARSE_LIB nsparse)
    endif()
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux AND AVX512_ENABLED)
    set(NSPARSE_OPT_LEVEL avx512)
    set(TARGET_LINK_NSPARSE_LIB nsparse_avx512)
    string(PREPEND LIB_EXT "_avx512")
else()
    set(NSPARSE_OPT_LEVEL avx2)
    set(TARGET_LINK_NSPARSE_LIB nsparse_avx2)
    string(PREPEND LIB_EXT "_avx2")
endif()

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp EXCLUDE_FROM_ALL)

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

cmake_minimum_required(VERSION 3.22...3.27)

project(NeuralSearch_JNI)

# ---------------------------------- SETUP ----------------------------------
set(TARGET_LIB_NSPARSE opensearch_neuralsearch_nsparse)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
option(CONFIG_TEST "Build tests (downloads googletest)" OFF)

# Set OS specific variables
if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    set(CMAKE_MACOSX_RPATH 1)
    set(JVM_OS_TYPE darwin)
    set(LIB_EXT .jnilib)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(JVM_OS_TYPE linux)
    set(LIB_EXT .so)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Windows)
    set(CXX_COMPILER_VERSION ${CMAKE_CXX_COMPILER_VERSION})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")
    set(JVM_OS_TYPE win32)
    set(LIB_EXT .dll)
    set(CMAKE_SHARED_LIBRARY_PREFIX "")
    set(CMAKE_STATIC_LIBRARY_PREFIX "")
else()
    message(FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()

# By default, set to 0.0.0
if(NOT NEURALSEARCH_PLUGIN_VERSION)
    set(NEURALSEARCH_PLUGIN_VERSION "0.0.0")
endif()

# Set architecture specific variables
if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
    set(MACH_ARCH arm64)
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
    set(MACH_ARCH x64)
endif()
# ----------------------------------------------------------------------------

# ---------------------------------- NSPARSE ----------------------------------
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/init-nsparse.cmake)
find_package(OpenMP REQUIRED)
add_library(
    ${TARGET_LIB_NSPARSE} SHARED
    ${CMAKE_CURRENT_SOURCE_DIR}/src/org_opensearch_neuralsearch_jni_NsparseService.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/nsparse_wrapper.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/nsparse_stream_writer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/jni_util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/commons.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/org_opensearch_neuralsearch_jni_JNICommons.cpp
)
target_link_libraries(${TARGET_LIB_NSPARSE} ${TARGET_LINK_NSPARSE_LIB} OpenMP::OpenMP_CXX)
target_include_directories(${TARGET_LIB_NSPARSE} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    $ENV{JAVA_HOME}/include
    $ENV{JAVA_HOME}/include/${JVM_OS_TYPE}
    ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp
)
set_target_properties(${TARGET_LIB_NSPARSE} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${TARGET_LIB_NSPARSE} PROPERTIES POSITION_INDEPENDENT_CODE ON)
# ---------------------------------------------------------------------------

# --------------------------------- TESTS -----------------------------------
if ("${WIN32}" STREQUAL "")
    if (CONFIG_TEST)
        # Reference - https://crascit.com/2015/07/25/cmake-gtest/
        configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
        execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
                WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download"
                )
        execute_process(COMMAND "${CMAKE_COMMAND}" --build .
                WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download"
                )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

        add_subdirectory("${CMAKE_BINARY_DIR}/googletest-src"
                "${CMAKE_BINARY_DIR}/googletest-build" EXCLUDE_FROM_ALL
                )
        add_executable(
                jni_test
                tests/nsparse_wrapper_test.cpp
                tests/test_util.cpp
                tests/commons_test.cpp
        )

        target_link_libraries(
                jni_test
                gtest_main
                gmock_main
                ${TARGET_LINK_NSPARSE_LIB}
                OpenMP::OpenMP_CXX
                ${TARGET_LIB_NSPARSE}
        )

        target_include_directories(jni_test PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/tests
                ${CMAKE_CURRENT_SOURCE_DIR}/include
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                $ENV{JAVA_HOME}/include
                $ENV{JAVA_HOME}/include/${JVM_OS_TYPE}
                ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp
                ${gtest_SOURCE_DIR}/include
                ${gmock_SOURCE_DIR}/include)

        set_target_properties(jni_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)
    endif ()
endif()
# ---------------------------------------------------------------------------

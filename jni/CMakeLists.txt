#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

cmake_minimum_required(VERSION 3.24.0)

project(NeuralSearch_JNI)

# ---------------------------------- SETUP ----------------------------------
set(TARGET_LIB_NSPARSE opensearch_neuralsearch_nsparse)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
option(CONFIG_TEST "Build tests (downloads googletest)" OFF)
option(ENABLE_SANITIZERS "Build with AddressSanitizer and LeakSanitizer" OFF)

# The JNI layer hands raw addresses across the language boundary and frees them on
# the native side, so leaks and use-after-free are the failure modes that test
# assertions cannot see. The sanitizer build is what actually covers them.
#
# UBSan is deliberately not enabled: abseil (pulled in by nsparse) fails to compile
# under -fsanitize=undefined on GCC, because the instrumentation defeats constexpr
# evaluation of the function-pointer comparisons in hash_policy_traits.h.
if (ENABLE_SANITIZERS)
    message(STATUS "Sanitizers enabled (address, leak)")
    add_compile_options(-fsanitize=address -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address)
endif()

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
)
target_link_libraries(${TARGET_LIB_NSPARSE} ${TARGET_LINK_NSPARSE_LIB} OpenMP::OpenMP_CXX)
target_include_directories(${TARGET_LIB_NSPARSE} PRIVATE
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
                ${CMAKE_CURRENT_SOURCE_DIR}/src
                $ENV{JAVA_HOME}/include
                $ENV{JAVA_HOME}/include/${JVM_OS_TYPE}
                ${CMAKE_CURRENT_SOURCE_DIR}/external/neural-sparse-cpp
                ${gtest_SOURCE_DIR}/include
                ${gmock_SOURCE_DIR}/include)

        # Emit into the build tree, not jni/bin, so `./gradlew clean` reclaims it
        # along with the rest of jni/build.
        if (ENABLE_SANITIZERS)
            # The ASan runtime lives beside the compiler rather than on the default
            # loader path on several distributions, so record its directory in the
            # test binary's RPATH instead of relying on ldconfig.
            execute_process(
                    COMMAND ${CMAKE_CXX_COMPILER} -print-file-name=libasan.so
                    OUTPUT_VARIABLE ASAN_RUNTIME_PATH
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
            get_filename_component(ASAN_RUNTIME_DIR "${ASAN_RUNTIME_PATH}" DIRECTORY)
            if (ASAN_RUNTIME_DIR)
                message(STATUS "ASan runtime directory: ${ASAN_RUNTIME_DIR}")
                set_target_properties(jni_test PROPERTIES BUILD_RPATH "${ASAN_RUNTIME_DIR}")
            endif()
        endif()

        set_target_properties(jni_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
    endif ()
endif()
# ---------------------------------------------------------------------------

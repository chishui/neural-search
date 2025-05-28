#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
cmake_minimum_required(VERSION 3.24.0)

project(NeuralSearchPlugin_JNI)
set(LIBRARY_NAME opensearch_sparse_ann)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
    set(MACH_ARCH arm64)
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
    set(MACH_ARCH x64)
endif()

# Enable universal binary build on macOS
if(APPLE)
    set(CMAKE_OSX_ARCHITECTURES "x86_64")

    execute_process(
        COMMAND /usr/libexec/java_home
        OUTPUT_VARIABLE JAVA_HOME
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    set(JAVA_INCLUDE_PATH "${JAVA_HOME}/include")
    set(JAVA_INCLUDE_PATH2 "${JAVA_HOME}/include/darwin")
    set(JAVA_AWT_INCLUDE_PATH "${JAVA_HOME}/include")
endif()

# Define output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Set source files
file(GLOB CPP_SOURCES "./src/*.cpp")

# Create the library target
add_library(${LIBRARY_NAME} SHARED ${CPP_SOURCES})

# Add include directories
target_include_directories(${LIBRARY_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Set compile options
target_compile_options(${LIBRARY_NAME} PRIVATE
    -O3
    -fvisibility=hidden
    -DNDEBUG
    -g0
)

# Find JNI
find_package(JNI REQUIRED)

# Add Java include directories
target_include_directories(${LIBRARY_NAME} PRIVATE ${JNI_INCLUDE_DIRS})

# Link against JVM library
target_link_libraries(${LIBRARY_NAME} PRIVATE ${JNI_LIBRARIES})

# Set JVM library path for runtime
if(UNIX AND NOT APPLE)
    set_target_properties(${LIBRARY_NAME} PROPERTIES
        INSTALL_RPATH "${JAVA_HOME}/lib/server"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

install(TARGETS ${LIBRARY_NAME}
    LIBRARY DESTINATION ${LIBRARY_NAME}
)

#
#   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   A copy of the License is located at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file. This file is distributed
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#   express or implied. See the License for the specific language governing
#   permissions and limitations under the License.
#

cmake_minimum_required(VERSION 2.8)

project(KNNIndexV2_0_6)

# Corner case. For CMake 2.8, there is no option to specify set(CMAKE_CXX_STANDARD 11). Instead, the flag manually needs
# to be set.
if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED True)
endif()

# Target Library to be built
set(KNN_INDEX KNNIndexV2_0_6)
set(KNN_PACKAGE_NAME opendistro-knnlib)

# Check if similarity search exists
find_path(NMS_REPO_DIR NAMES similarity_search PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib REQUIRED)

# If not, pull the updated submodule
if (NOT EXISTS ${NMS_REPO_DIR})
    message(STATUS "Could not find nmslib. Pulling updated submodule.")
    execute_process(COMMAND git submodule update --init -- external/nmslib WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

# Add the subdirectory so it is possible to use its targets
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search EXCLUDE_FROM_ALL)

# Set OS specific variables
if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    set(CMAKE_MACOSX_RPATH 1)
    set(JVM_OS_TYPE darwin)
    set(LIB_EXT .jnilib)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(JVM_OS_TYPE linux)
    set(LIB_EXT .so)
else()
    message( FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()

# Compile the library
add_library(${KNN_INDEX} SHARED ${CMAKE_CURRENT_SOURCE_DIR}/src/com_amazon_opendistroforelasticsearch_knn_index_v206_KNNIndex.cpp)
target_link_libraries(${KNN_INDEX} NonMetricSpaceLib)
target_include_directories(${KNN_INDEX} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/${JVM_OS_TYPE} ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search/include)

set_target_properties(${KNN_INDEX} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${KNN_INDEX} PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(${KNN_INDEX} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/release)

# Installation rules for shared library
install(TARGETS ${KNN_INDEX}
        LIBRARY DESTINATION lib
        COMPONENT library)

# CPack section to build artifacts
set(KNN_MAINTAINER "OpenDistro for Elasticsearch Team <opendistro@amazon.com>")
set(ODFE_DOWNLOAD_URL "https://opendistro.github.io/elasticsearch/downloads")
set(CPACK_PACKAGE_NAME ${KNN_PACKAGE_NAME})
set(CPACK_PACKAGE_VERSION 1.12.0.0)
set(CMAKE_INSTALL_PREFIX /usr)
set(CPACK_GENERATOR "RPM;DEB")
SET(CPACK_OUTPUT_FILE_PREFIX packages)
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "KNN JNI library built off of NMSLIB for OpenDistro for Elasticsearch. Reference documentation can be found at https://opendistro.github.io/for-elasticsearch-docs/.")
set(CPACK_PACKAGE_VENDOR "Amazon")
set(CPACK_PACKAGE_CONTACT "Maintainer: ${KNN_MAINTAINER}")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}_${JVM_OS_TYPE}.${CMAKE_SYSTEM_PROCESSOR}")

# RPM Specific variables
set(CPACK_RPM_PACKAGE_RELEASE ${CPACK_PACKAGE_RELEASE})
set(CPACK_RPM_PACKAGE_URL ${ODFE_DOWNLOAD_URL})
set(CPACK_RPM_PACKAGE_DESCRIPTION "Open Distro for Elasticsearch KNN JNI Library")
set(CPACK_RPM_PACKAGE_LICENSE "ASL-2.0")

# DEB Specific variables
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${ODFE_DOWNLOAD_URL})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${KNN_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_SOURCE ${CPACK_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_DEBIAN_PACKAGE_SECTION "libs")

include(CPack)
#
#   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

project(KNNPlugin_JNI)

# Corner case. For CMake 2.8, there is no option to specify set(CMAKE_CXX_STANDARD 11). Instead, the flag manually needs
# to be set.
if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED True)
endif()


# Set OS specific variables
if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    set(CMAKE_MACOSX_RPATH 1)
    set(JVM_OS_TYPE darwin)
    set(LIB_EXT .jnilib)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(JVM_OS_TYPE linux)
    set(LIB_EXT .so)
else()
    message(FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()

# Set architecture specific variables
if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
    set(MACH_ARCH arm64)
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
    set(MACH_ARCH x64)
endif()

# Add the subdirectories to build their targets
set(KNNIndex_NMSLIB KNNIndex_NMSLIB_V2_0_11)
set(NMSLIB_COMPONENT NMSLIB)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/nmslib)

set(KNNIndex_FAISS KNNIndex_FAISS_V1_6_5)
set(FAISS_COMPONENT FAISS)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/faiss)

# CPack Logic
set(KNN_MAINTAINER "OpenDistro for Elasticsearch Team <opendistro@amazon.com>")
set(ODFE_DOWNLOAD_URL "https://opendistro.github.io/for-elasticsearch/downloads.html")
set(CPACK_PACKAGE_VERSION 1.13.0.0)
set(CMAKE_INSTALL_PREFIX /usr)
set(CPACK_GENERATOR "RPM;DEB")
set(CPACK_OUTPUT_FILE_PREFIX packages)
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_VENDOR "Amazon")
set(CPACK_PACKAGE_CONTACT "Maintainer: ${KNN_MAINTAINER}")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_COMPONENTS_GROUPING IGNORE)
get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
list(REMOVE_ITEM CPACK_COMPONENTS_ALL "Unspecified")

## RPM variables
set(CPACK_RPM_PACKAGE_LICENSE "ASL-2.0")
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_PACKAGE_URL ${ODFE_DOWNLOAD_URL})
set(CPACK_RPM_PACKAGE_RELEASE ${CPACK_PACKAGE_RELEASE})

## DEB variables
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${ODFE_DOWNLOAD_URL})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${KNN_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_DEBIAN_PACKAGE_SECTION "libs")
set(CPACK_DEB_COMPONENT_INSTALL ON)

## Component specific variable
set(KNN_NMSLIB_PACKAGE_NAME opendistro-knnlib-nmslib)
set(KNN_FAISS_PACKAGE_NAME opendistro-knnlib-faiss)

### Nmslib
set(NMSLIB_PACKAGE_DESCRIPTION "KNN JNI library built off of nmslib for OpenDistro for Elasticsearch. Reference documentation can be found at https://opendistro.github.io/for-elasticsearch-docs/docs/knn/.")
set(FAISS_PACKAGE_DESCRIPTION "KNN JNI library built off of faiss for OpenDistro for Elasticsearch. Reference documentation can be found at https://opendistro.github.io/for-elasticsearch-docs/docs/knn/.")

#### Debian
set(CPACK_DEBIAN_${NMSLIB_COMPONENT}_PACKAGE_NAME ${KNN_NMSLIB_PACKAGE_NAME})
set(CPACK_DEBIAN_${NMSLIB_COMPONENT}_FILE_NAME "${CPACK_DEBIAN_${NMSLIB_COMPONENT}_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${JVM_OS_TYPE}-${MACH_ARCH}.deb")
set(CPACK_DEBIAN_${NMSLIB_COMPONENT}_DESCRIPTION ${NMSLIB_PACKAGE_DESCRIPTION})
set(CPACK_DEBIAN_${NMSLIB_COMPONENT}_PACKAGE_SOURCE ${CPACK_DEBIAN_${NMSLIB_COMPONENT}_PACKAGE_NAME})

#### RPM
set(CPACK_RPM_${NMSLIB_COMPONENT}_PACKAGE_NAME ${KNN_NMSLIB_PACKAGE_NAME})
set(CPACK_RPM_${NMSLIB_COMPONENT}_FILE_NAME "${CPACK_RPM_${NMSLIB_COMPONENT}_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${JVM_OS_TYPE}-${MACH_ARCH}.rpm")
set(CPACK_RPM_${NMSLIB_COMPONENT}_PACKAGE_DESCRIPTION ${NMSLIB_PACKAGE_DESCRIPTION})
set(CPACK_RPM_${NMSLIB_COMPONENT}_PACKAGE_SUMMARY "Open Distro for Elasticsearch k-NN JNI Library with nmslib")

### Faiss
#### Debian
set(CPACK_DEBIAN_${FAISS_COMPONENT}_PACKAGE_NAME ${KNN_FAISS_PACKAGE_NAME})
set(CPACK_DEBIAN_${FAISS_COMPONENT}_FILE_NAME "${CPACK_DEBIAN_${FAISS_COMPONENT}_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${JVM_OS_TYPE}-${MACH_ARCH}.deb")
set(CPACK_DEBIAN_${FAISS_COMPONENT}_DESCRIPTION "KNN JNI library built off of FAISS for OpenDistro for Elasticsearch. Reference documentation can be found at https://opendistro.github.io/for-elasticsearch-docs/docs/knn/.")
set(CPACK_DEBIAN_${FAISS_COMPONENT}_PACKAGE_SOURCE ${CPACK_DEBIAN_${FAISS_COMPONENT}_PACKAGE_NAME})

#### RPM
set(CPACK_RPM_${FAISS_COMPONENT}_PACKAGE_NAME ${KNN_FAISS_PACKAGE_NAME})
set(CPACK_RPM_${FAISS_COMPONENT}_FILE_NAME "${CPACK_RPM_${FAISS_COMPONENT}_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${JVM_OS_TYPE}-${MACH_ARCH}.rpm")
set(CPACK_RPM_${FAISS_COMPONENT}_PACKAGE_DESCRIPTION ${FAISS_PACKAGE_DESCRIPTION})
set(CPACK_RPM_${FAISS_COMPONENT}_PACKAGE_SUMMARY "Open Distro for Elasticsearch k-NN JNI Library with faiss")

include(CPack)

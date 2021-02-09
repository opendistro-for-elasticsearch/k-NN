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
    message( FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()


if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
    set(MACH_ARCH arm64)
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
    set(MACH_ARCH x64)
endif()

set(KNN_MAINTAINER "OpenDistro for Elasticsearch Team <opendistro@amazon.com>")
set(ODFE_DOWNLOAD_URL "https://opendistro.github.io/for-elasticsearch/downloads.html")
set(CPACK_PACKAGE_VERSION 1.13.0.0)
set(CMAKE_INSTALL_PREFIX /usr)
set(CPACK_GENERATOR "RPM;DEB")
SET(CPACK_OUTPUT_FILE_PREFIX packages)
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_PACKAGE_VENDOR "Amazon")
set(CPACK_PACKAGE_CONTACT "Maintainer: ${KNN_MAINTAINER}")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})

# RPM Specific variables
set(CPACK_RPM_PACKAGE_LICENSE "ASL-2.0")

# DEB Specific variables
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${ODFE_DOWNLOAD_URL})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${KNN_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_DEBIAN_PACKAGE_SECTION "libs")

#include(CPack)

# Add the subdirectory so it is possible to use its targets
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/nmslib)

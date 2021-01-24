cmake_minimum_required(VERSION 2.8)

project(KNNIndex_FAISS)

# Corner case. For CMake 2.8, there is no option to specify set(CMAKE_CXX_STANDARD 11). Instead, the flag manually needs
# to be set.
if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED True)
endif()

# Target Library to be built
set(KNN_INDEX KNNIndex_FAISS_V1_6_5)
set(KNN_PACKAGE_NAME opendistro-knnlib)

# --- FAISS BEGIN ---
if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
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

find_package(OpenMP REQUIRED)
find_package(ZLIB REQUIRED)
find_package(BLAS REQUIRED)
find_package(LAPACK REQUIRED)

# Check if faiss search exists
find_path(FAISS_REPO_DIR NAMES faiss PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/faiss)

# If not, pull the updated submodule
if (NOT EXISTS ${FAISS_REPO_DIR})
    message(STATUS "Could not find faiss. Pulling updated submodule.")
    execute_process(COMMAND git submodule update --init -- external/faiss WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

# Add the subdirectory so it is possible to use its targets
set(FAISS_ENABLE_GPU OFF)
set(FAISS_ENABLE_PYTHON OFF)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/faiss EXCLUDE_FROM_ALL)
# --- FAISS END

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
include_directories($ENV{JAVA_HOME}/include )
add_library(${KNN_INDEX} SHARED ${CMAKE_CURRENT_SOURCE_DIR}/src/com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFIndex.cpp)
target_link_libraries(${KNN_INDEX} faiss ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} OpenMP::OpenMP_CXX)
target_include_directories(${KNN_INDEX} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/${JVM_OS_TYPE} ${CMAKE_CURRENT_SOURCE_DIR}/external/faiss/faiss )

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
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "KNN JNI library built off of Faiss for OpenDistro for Elasticsearch. Reference documentation can be found at https://opendistro.github.io/for-elasticsearch-docs/.")
set(CPACK_PACKAGE_VENDOR "Amazon")
set(CPACK_PACKAGE_CONTACT "Maintainer: ${KNN_MAINTAINER}")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_PACKAGE_RELEASE}_${JVM_OS_TYPE}.${CMAKE_SYSTEM_PROCESSOR}")

# RPM Specific variables
set(CPACK_RPM_PACKAGE_RELEASE ${CPACK_PACKAGE_RELEASE})
set(CPACK_RPM_PACKAGE_URL ${ODFE_DOWNLOAD_URL})
set(CPACK_RPM_PACKAGE_DESCRIPTION "Open Distro for Elasticsearch KNN Faiss JNI Library")
set(CPACK_RPM_PACKAGE_LICENSE "ASL-2.0")

# DEB Specific variables
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${ODFE_DOWNLOAD_URL})
set(CPACK_DEBIAN_PACKAGE_MAINTAINER ${KNN_MAINTAINER})
set(CPACK_DEBIAN_PACKAGE_SOURCE ${CPACK_PACKAGE_NAME})
set(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
set(CPACK_DEBIAN_PACKAGE_SECTION "libs")

include(CPack)

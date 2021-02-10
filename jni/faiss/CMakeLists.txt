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

# Target Library to be built
set(KNN_INDEX ${KNNIndex_FAISS})

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

# Compile the library
include_directories($ENV{JAVA_HOME}/include )
add_library(${KNN_INDEX} SHARED ${CMAKE_CURRENT_SOURCE_DIR}/src/com_amazon_opendistroforelasticsearch_knn_index_faiss_v165_KNNFaissIndex.cpp ${PROJECT_SOURCE_DIR}/src/jni_util.cpp)
target_link_libraries(${KNN_INDEX} faiss ${BLAS_LIBRARIES} ${LAPACK_LIBRARIES} OpenMP::OpenMP_CXX)
target_include_directories(${KNN_INDEX} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/${JVM_OS_TYPE} ${CMAKE_CURRENT_SOURCE_DIR}/external/faiss/faiss ${PROJECT_SOURCE_DIR}/include)

set_target_properties(${KNN_INDEX} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${KNN_INDEX} PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(${KNN_INDEX} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/release)

# Installation rules for shared library
install(TARGETS ${KNN_INDEX}
        LIBRARY DESTINATION lib
        COMPONENT ${FAISS_COMPONENT})

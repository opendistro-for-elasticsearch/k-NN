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
set(KNN_INDEX ${KNNIndex_NMSLIB})

# Check if similarity search exists
find_path(NMS_REPO_DIR NAMES similarity_search PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib)

# If not, pull the updated submodule
if (NOT EXISTS ${NMS_REPO_DIR})
    message(STATUS "Could not find nmslib. Pulling updated submodule.")
    execute_process(COMMAND git submodule update --init -- external/nmslib WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

# Add the subdirectory so it is possible to use its targets
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search EXCLUDE_FROM_ALL)

# Compile the library
add_library(${KNN_INDEX} SHARED ${CMAKE_CURRENT_SOURCE_DIR}/src/com_amazon_opendistroforelasticsearch_knn_index_nmslib_v2011_KNNNmsLibIndex.cpp ${PROJECT_SOURCE_DIR}/src/jni_util.cpp)
target_link_libraries(${KNN_INDEX} NonMetricSpaceLib)
target_include_directories(${KNN_INDEX} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/${JVM_OS_TYPE} ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search/include ${PROJECT_SOURCE_DIR}/include)

set_target_properties(${KNN_INDEX} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${KNN_INDEX} PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(${KNN_INDEX} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/release)

# Installation rules for shared library
install(TARGETS ${KNN_INDEX}
        LIBRARY DESTINATION lib
        COMPONENT ${NMSLIB_COMPONENT})

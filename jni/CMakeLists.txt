cmake_minimum_required(VERSION 3.10)

project(KNNIndexV1_7_3_6)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Target Library to be built
set(KNN_INDEX KNNIndexV1_7_3_6)

find_path(NMS_REPO_DIR NAMES nmslib PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external REQUIRED)

if (NOT EXISTS ${NMS_REPO_DIR})
    message(STATUS "Could not find nmslib. Pulling updating submodule.")
    execute_process(COMMAND git submodule update --init -- external/nmslib WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif ()

## Check if nmslib is built and if not build it
#find_library(NMS_LIB NAMES libNonMetricSpaceLib.a  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search/release REQUIRED)

add_subdirectory(external/nmslib/similarity_search)

if (${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    set(JVM_OS_TYPE darwin)
    set(LIB_EXT .jnilib)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    set(JVM_OS_TYPE linux)
    set(LIB_EXT .so)
else()
    message( FATAL_ERROR "Unable to run on system: ${CMAKE_SYSTEM_NAME}")
endif()

# Build knn lib
add_library(${KNN_INDEX} SHARED ${CMAKE_CURRENT_SOURCE_DIR}/src/v1736/com_amazon_opendistroforelasticsearch_knn_index_v1736_KNNIndex.cpp)
target_link_libraries(${KNN_INDEX} NonMetricSpaceLib)
target_include_directories(${KNN_INDEX} PRIVATE $ENV{JAVA_HOME}/include $ENV{JAVA_HOME}/include/${JVM_OS_TYPE} ${CMAKE_CURRENT_SOURCE_DIR}/external/nmslib/similarity_search/include)

set_target_properties(${KNN_INDEX} PROPERTIES SUFFIX ${LIB_EXT})
set_target_properties(${KNN_INDEX} PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(${KNN_INDEX} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../buildSrc)
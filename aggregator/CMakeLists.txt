cmake_minimum_required(VERSION 3.9.1)
project(aggregator)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_FLAGS "-O3")
# Set the AWS service components used by this project.
set(SERVICE_COMPONENTS s3)
#set(AWSSDK_DIR aws/lib/cmake/AWSSDK)
#set(aws-cpp-sdk-core_DIR aws/src/aws-cpp-sdk-core)
#set(aws-crt-cpp_DIR aws/lib/aws-crt-cpp/cmake)
#set(aws-c-http_DIR aws/lib/aws-c-http/cmake)
#set(aws-c-io_DIR aws/lib/aws-c-io/cmake)
#include_directories(aws/lib)
#include_directories(aws/src)
 
#include_directories(jsoncpp/dist)
include_directories(emhash)
#include("${PROJECT_SOURCE_DIR}/AnyBlob/anyblob.cmake")
#include_directories(SYSTEM ${ANYBLOB_INCLUDE_DIR})

find_package(AWSSDK REQUIRED COMPONENTS ${SERVICE_COMPONENTS})
find_package(Threads REQUIRED)
find_package(ZLIB QUIET)

#add_library(Aws aws/include)
#set_property(TARGET aggregator PROPERTY CXX_STANDART 23)
#add_executable(aggregator text.cpp jsoncpp/dist/json/json.h jsoncpp/dist/json/json-forwards.h jsoncpp/dist/jsoncpp.cpp emhash/hash_table8.hpp)
add_executable(aggregator text.cpp emhash/hash_table8.hpp)
target_link_libraries(aggregator PUBLIC Threads::Threads ${AWSSDK_LINK_LIBRARIES})# AnyBlob)
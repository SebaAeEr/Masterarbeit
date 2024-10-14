cmake_minimum_required(VERSION 3.9.1)
project(aggregator)
set(CMAKE_CXX_STANDARD 14)
add_executable(aggregator text.cpp jsoncpp/dist/json/json.h jsoncpp/dist/json/json-forwards.h jsoncpp/dist/jsoncpp.cpp)
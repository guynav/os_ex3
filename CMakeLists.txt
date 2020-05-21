cmake_minimum_required(VERSION 3.10)
project(ex3_os)

set(CMAKE_CXX_STANDARD 11)

add_executable(ex3_os main.cpp Barrier.cpp Barrier.h MapReduceFramework.h MapReduceFramwork.cpp)
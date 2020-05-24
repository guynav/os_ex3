cmake_minimum_required(VERSION 3.10)
project(ex3_os)

set(CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
add_executable(ex3_os main.cpp Barrier.cpp Barrier.h MapReduceFramework.h MapReduceFramwork.cpp)
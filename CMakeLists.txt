cmake_minimum_required(VERSION 3.10)
project(os_ex3)

set(CMAKE_CXX_STANDARD 11)
set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
add_executable(os_ex3 Barrier.cpp Barrier.h MapReduceFramework.h MapReduceFramwork.cpp SampleClient.cpp)
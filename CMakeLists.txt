#cmake_minimum_required(VERSION 3.10)
#project(os_ex3)
#
#set(CMAKE_CXX_STANDARD 11)
#set(GCC_COVERAGE_COMPILE_FLAGS "-pthread")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )
##set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer ")#-fsanitize=thread")
##set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer ")# -fsanitize=thread")
##set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -O1 -fno-omit-frame-pointer -g")
#add_executable(os_ex3 Barrier.cpp Barrier.h MapReduceFramework.h MapReduceFramework.cpp  FileWordCounter.cpp Eyuni.cpp)

cmake_minimum_required(VERSION 3.1)


# NOTE: You can't have both ThreadSanitizer and AddressSanitizer enabled at the same time.

# Uncomment the following to enable ThreadSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=thread")

# Uncomment the following to enable AddressSanitizer.
#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")


# Project configuration
project(ex3 VERSION 1.0 LANGUAGES C CXX)


# Ensure system has pthreads
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_library(MapReduceFramework
        MapReduceClient.h
        MapReduceFramework.cpp MapReduceFramework.h
        Barrier.cpp
        Barrier.h
        )


set_property(TARGET MapReduceFramework PROPERTY CXX_STANDARD 11)
target_compile_options(MapReduceFramework PUBLIC -Wall -Wextra)
target_include_directories(MapReduceFramework PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# link pthreads to your framework
target_link_libraries(MapReduceFramework PUBLIC Threads::Threads)

# Add tests
add_subdirectory(mattanTests)
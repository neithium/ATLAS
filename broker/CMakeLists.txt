cmake_minimum_required(VERSION 3.10)
project(tvmjns-broker VERSION 1.0.0 LANGUAGES CXX)

# C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compiler flags for optimization and warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

# Include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../common
)

# Source files
set(SOURCES
    src/main.cpp
    src/tcp_server.cpp
    src/thread_pool.cpp
    src/logger.cpp
)

# Create executable
add_executable(tvmjns-broker ${SOURCES})

# Link pthread
find_package(Threads REQUIRED)
target_link_libraries(tvmjns-broker PRIVATE Threads::Threads)

# Installation
install(TARGETS tvmjns-broker DESTINATION bin)

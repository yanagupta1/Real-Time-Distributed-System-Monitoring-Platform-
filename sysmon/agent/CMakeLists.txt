cmake_minimum_required(VERSION 3.18)
project(sysmon_agent VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type")

# Compiler flags for performance
add_compile_options(
    -O3
    -march=native
    -Wall
    -Wextra
    -Wno-unused-parameter
)

# Find dependencies
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka)

# nlohmann/json (header-only, fetched via FetchContent)
include(FetchContent)
FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(nlohmann_json)

# Agent executable
add_executable(sysmon_agent
    src/main.cpp
    src/metrics_collector.cpp
    src/kafka_producer.cpp
)

target_include_directories(sysmon_agent PRIVATE
    src
    ${RDKAFKA_INCLUDE_DIRS}
)

target_link_libraries(sysmon_agent PRIVATE
    Threads::Threads
    ${RDKAFKA_LIBRARIES}
    nlohmann_json::nlohmann_json
)

# Optional: sanitizer builds
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
if(ENABLE_ASAN)
    target_compile_options(sysmon_agent PRIVATE -fsanitize=address,undefined)
    target_link_options(sysmon_agent PRIVATE -fsanitize=address,undefined)
endif()

install(TARGETS sysmon_agent DESTINATION bin)

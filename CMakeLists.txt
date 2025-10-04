cmake_minimum_required(VERSION 3.1...3.26)

project(
  ModernCMakeExample
  VERSION 1.0
  LANGUAGES CXX)

set (CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITHOUT_NUMPY")
set(CMAKE_BUILD_TYPE RELEASE)
# set(CMAKE_BUILD_TYPE DEBUG)


set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.71.0 COMPONENTS program_options) 

add_subdirectory(deps/spdlog)
add_subdirectory(deps/json)

include_directories(include)

# any python version
find_package(PythonLibs REQUIRED)
include_directories(${PYTHON_INCLUDE_DIRS})
message(STATUS "PYTHON_INCLUDE_DIRS = ${PYTHON_INCLUDE_DIRS}")


set(SOURCES src/psim.cc
            src/loadbalancer.cc
            src/context.cc
            src/bwalloc.cc
            src/network.cc
            src/core_network.cc
            src/traffic_gen.cc
            src/protocol_builder.cc
            src/flow.cc
            src/options.cc
            src/protocol.cc
            src/main.cc)

add_executable(psim ${SOURCES})

target_link_libraries(psim ${PYTHON_LIBRARIES} spdlog::spdlog)
target_link_libraries(psim nlohmann_json::nlohmann_json)

if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS}) 
  target_link_libraries(psim ${Boost_LIBRARIES})
endif()

add_executable(scheduler
               src/scheduler/main.cc
               src/scheduler/scheduler.cc)

target_link_libraries(scheduler nlohmann_json::nlohmann_json)

if (Boost_FOUND)
  target_link_libraries(scheduler ${Boost_LIBRARIES})
endif()
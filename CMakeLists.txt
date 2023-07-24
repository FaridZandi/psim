cmake_minimum_required(VERSION 3.1...3.26)

project(
  ModernCMakeExample
  VERSION 1.0
  LANGUAGES CXX)

set (CMAKE_CXX_STANDARD 11)
# set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DWITHOUT_NUMPY")


set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 

find_package(Boost 1.71.0 COMPONENTS serialization program_options) 


include_directories(include)

# any python version
find_package(PythonLibs REQUIRED)

include_directories(${PYTHON_INCLUDE_DIRS})
message(STATUS "PYTHON_INCLUDE_DIRS = ${PYTHON_INCLUDE_DIRS}")

set(SOURCES src/main.cc 
            src/psim.cc
            src/network.cc
            src/protocol_builder.cc
            src/protocol.cc)

add_executable(psim ${SOURCES})

target_link_libraries(psim ${PYTHON_LIBRARIES})

if (Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS}) 
  target_link_libraries(psim ${Boost_LIBRARIES})
endif()
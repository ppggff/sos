cmake_minimum_required(VERSION 3.20)
project(sos)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_VERBOSE_MAKEFILE ON)

add_executable(sos hash3.c hash3.h codec.h sqlite/sqlite3.amalgamation.c sos.cc)
target_link_libraries(sos ${CMAKE_DL_LIBS})

cmake_minimum_required(VERSION 3.20)
project(sos VERSION 0.2.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(CPACK_SOURCE_GENERATOR "TGZ")
set(CPACK_SOURCE_IGNORE_FILES
        \\.git/
        build/
        data/
        ".*~$"
        )
set(CPACK_VERBATIM_VARIABLES YES)
include(CPack)

add_executable(sos hash3.c hash3.h codec.h sqlite/sqlite3.amalgamation.c sos.cc)
target_link_libraries(sos ${CMAKE_DL_LIBS})

install(TARGETS sos DESTINATION bin)
install(FILES template.sqlite DESTINATION data)

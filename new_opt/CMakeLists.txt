cmake_minimum_required(VERSION 3.20)

project(new_opt
    VERSION 0.1.0
    DESCRIPTION "Experimental optimizer components"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(NEW_OPT_BUILD_TESTS "Build the new_opt tests" ON)
option(NEW_OPT_ENABLE_WARNINGS "Enable compiler warnings for new_opt targets" ON)

# The implementation is currently header-only. Keeping an interface target
# gives future .cpp files and downstream experiments one stable dependency.
add_library(new_opt_headers INTERFACE)
add_library(new_opt::headers ALIAS new_opt_headers)
target_include_directories(new_opt_headers
    INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

if(NEW_OPT_BUILD_TESTS)
    enable_testing()

    add_executable(new_opt_smoke
        test/smoke.cpp
    )
    target_link_libraries(new_opt_smoke PRIVATE new_opt::headers)

    add_executable(new_opt_list_tests
        test/list_test.cpp
    )
    target_link_libraries(new_opt_list_tests PRIVATE new_opt::headers)

    if(NEW_OPT_ENABLE_WARNINGS)
        foreach(test_target IN ITEMS new_opt_smoke new_opt_list_tests)
            if(MSVC)
                target_compile_options(${test_target} PRIVATE /W4)
            else()
                target_compile_options(${test_target} PRIVATE -Wall -Wextra -Wpedantic)
            endif()
        endforeach()
    endif()

    add_test(NAME new_opt_smoke COMMAND new_opt_smoke)
    add_test(NAME new_opt_list_tests COMMAND new_opt_list_tests)
endif()

include(GNUInstallDirs)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS new_opt_headers EXPORT new_optTargets)
install(EXPORT new_optTargets
    FILE new_optTargets.cmake
    NAMESPACE new_opt::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/new_opt
)

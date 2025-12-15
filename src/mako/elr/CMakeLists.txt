# CMakeLists.txt for Early Lock Release (ELR) module
#
# This module implements Early Lock Release for reduced contention
# in geo-replicated Mako.

# ELR source files
set(ELR_SOURCES
    elr_manager.cc
    dependency_tracker.cc
    cascade_abort.cc
    elr_log.cc
    elr_integration.cc
)

# ELR header files
set(ELR_HEADERS
    elr_common.h
    elr_manager.h
    dependency_tracker.h
    cascade_abort.h
    elr_log.h
    elr_integration.h
)

# Create ELR library
add_library(mako_elr STATIC ${ELR_SOURCES} ${ELR_HEADERS})

# Set include directories
target_include_directories(mako_elr PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)

# Set compile definitions
target_compile_definitions(mako_elr PUBLIC ENABLE_ELR)

# Set C++17 standard
target_compile_features(mako_elr PUBLIC cxx_std_17)

# Link dependencies (if any)
# target_link_libraries(mako_elr PUBLIC ...)

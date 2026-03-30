cmake_minimum_required(VERSION 3.16)
project(MiniSQL VERSION 1.0.0 LANGUAGES CXX)

# ========================== C++ Standard ==========================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ========================== Output Directories ==========================
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# ========================== Compiler Flags ==========================
if (MSVC)
    add_compile_options(/W4 /WX)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ========================== Source Files ==========================
# Automatically find all .cpp files recursively (Recommended for your structure)
file(GLOB_RECURSE SOURCES 
    "src/*.cpp"
)

# ========================== Executable ==========================
add_executable(minisql ${SOURCES})

# ========================== Include Directories ==========================
target_include_directories(minisql PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# ========================== Print Summary ==========================
message(STATUS "MiniSQL Project Configuration")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Source files found: ${SOURCES}")
message(STATUS "Executable will be in: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/minisql")
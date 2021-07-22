cmake_minimum_required(VERSION 3.16)
project(map_reduce C)

set(CMAKE_C_STANDARD 99)

add_compile_options(-Wall -Werror)

add_library(mapreduce dict.c mapreduce.c)

add_executable(main main.c)
target_link_libraries(main mapreduce pthread)
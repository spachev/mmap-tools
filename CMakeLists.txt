cmake_minimum_required(VERSION 2.8.11)
project(mmap_tools)
set(CMAKE_CXX_FLAGS "--std=c++11 -g -O3")
add_executable(mysqldump2csv mysqldump2csv.cc mmap_base.cc iobuf.cc)

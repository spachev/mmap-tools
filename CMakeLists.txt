cmake_minimum_required(VERSION 2.8.11)
project(mmap_tools)
add_executable(mysqldump2csv mysqldump2csv.cc mmap_base.cc)

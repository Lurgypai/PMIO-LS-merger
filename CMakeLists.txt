cmake_minimum_required(VERSION 3.20)

project(SmartMerge)

add_executable(smartMerge "smartMerge.cpp" "merger.cpp" "mergerItem.cpp")
add_executable(logMetadata "logMetadata.cpp")
add_executable(initMetadataFiles "initMetadataFiles.cpp")
add_library(mergeThread "mergeThread.cpp" "merger.cpp" "mergerItem.cpp")

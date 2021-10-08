# CMakeLists.txt for the "mrtools" package.
# It creates a shared library with a ROOT dictionary. 
cmake_minimum_required(VERSION 3.0 FATAL_ERROR)

project(mrtools LANGUAGES CXX)

find_package(ROOT REQUIRED COMPONENTS ROOTVecOps)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
set( CMAKE_INSTALL_LIBDIR ${CMAKE_LIBRARY_OUTPUT_DIRECTORY} )

root_generate_dictionary(G__MRTools MRTools/HelloWorld.hxx LINKDEF LinkDef.h)
add_library(MRTools SHARED HelloWorld.cxx G__MRTools.cxx)
target_link_libraries(MRTools PUBLIC ROOT::ROOTVecOps)

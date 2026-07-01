# h5shim: pybind11 module over sliderule's C++ H5Coro (zagg issue #149 phase 3).
#
# Expects an already-configured/built sliderule tree (SHARED_LIBRARY=ON):
#   -DSLIDERULE_SRC=<sliderule source root>   (the sed-patched copy, see Containerfile.shim)
#   -DSLIDERULE_BUILD=<sliderule cmake build dir>  (for auto/platform.h + libsliderule.so)
# pybind11 is resolved from the active python env (pip install pybind11):
#   -Dpybind11_DIR=$(python -c "import pybind11; print(pybind11.get_cmake_dir())")
cmake_minimum_required(VERSION 3.13.0)
project(h5shim LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED SLIDERULE_SRC OR NOT DEFINED SLIDERULE_BUILD)
    message(FATAL_ERROR "pass -DSLIDERULE_SRC=... and -DSLIDERULE_BUILD=...")
endif()

find_package(pybind11 REQUIRED)
find_package(Lua "5.3" REQUIRED)  # LuaObject.h in the include chain needs lua headers

pybind11_add_module(h5shim shim.cpp)

target_include_directories(h5shim PRIVATE
    ${SLIDERULE_SRC}/packages/core/package
    ${SLIDERULE_SRC}/packages/h5coro/package
    ${SLIDERULE_SRC}/platforms/linux
    ${SLIDERULE_BUILD}/auto            # generated platform.h (__le__ etc.)
    ${LUA_INCLUDE_DIR}
)

find_library(SLIDERULE_LIB sliderule PATHS ${SLIDERULE_BUILD} NO_DEFAULT_PATH REQUIRED)
target_link_libraries(h5shim PRIVATE ${SLIDERULE_LIB})

# match the H5CORO_THREAD_POOL_SIZE the library was configured with, so the
# header-visible constants agree with the built lib
if(DEFINED H5CORO_THREAD_POOL_SIZE)
    target_compile_definitions(h5shim PRIVATE H5CORO_THREAD_POOL_SIZE=${H5CORO_THREAD_POOL_SIZE})
endif()

# RequestParameters.h (reached via H5CoroLib.h) consumes the BUILDINFO define
# that sliderule's server application normally injects
target_compile_definitions(h5shim PRIVATE BUILDINFO="zagg-benchmark-shim")

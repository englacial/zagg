/*
 * h5shim — benchmark-only pybind11 wrapper over sliderule's C++ H5Coro
 * (phase 3 of zagg issue #149). Exposes exactly what bench_replay.py needs:
 *
 *   read(resource, [(dataset, start, end) | (dataset, None, None), ...])
 *       -> list[np.ndarray]
 *
 * `resource` is an absolute path to a local .h5 granule; (start, end) is a
 * half-open row range on dimension 0 (remaining dims are read in full, which
 * H5Dataset does natively when slicendims < ndims); (None, None) reads the
 * whole dataset. Arrays come back in the file's stored type (DYNAMIC valtype
 * = no translation in H5Coro::read) and are copied into numpy, after which
 * the H5Coro buffer is freed with the matching aligned operator delete[].
 *
 * Not a production binding: single Context per call, serial reads, assets
 * cached per directory and never freed (they are LuaObjects; constructed with
 * L == NULL, which LuaObject tolerates). The Asset constructor/attributes_t
 * are private upstream — the container build applies
 * sliderule-minimal-build.patch to make that section public (see build.sh).
 */

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <dlfcn.h>

#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "Asset.h"
#include "FileIODriver.h"
#include "H5CoroLib.h"
#include "RecordObject.h"

void initcore(void);  // C++ linkage (packages/core/package/core.h)
extern "C" void inith5coro(void);  // packages/h5coro/package/h5coro.h

namespace py = pybind11;

namespace {

// one Asset per granule directory, deliberately leaked (benchmark process)
Asset* asset_for_dir(const std::string& dir)
{
    static std::map<std::string, Asset*> cache;
    auto it = cache.find(dir);
    if (it != cache.end()) return it->second;
    Asset::attributes_t attrs = {"bench", "bench", "file", dir.c_str(), NULL, NULL};
    const Asset::io_driver_t driver = {FileIODriver::create};
    Asset* asset = new Asset(NULL, attrs, driver);  // Asset duplicates the strings
    cache[dir] = asset;
    return asset;
}

py::dtype np_dtype(RecordObject::fieldType_t ft, const char* dataset)
{
    switch (ft)
    {
        case RecordObject::INT8:   return py::dtype("i1");
        case RecordObject::INT16:  return py::dtype("i2");
        case RecordObject::INT32:  return py::dtype("i4");
        case RecordObject::INT64:  return py::dtype("i8");
        case RecordObject::UINT8:  return py::dtype("u1");
        case RecordObject::UINT16: return py::dtype("u2");
        case RecordObject::UINT32: return py::dtype("u4");
        case RecordObject::UINT64: return py::dtype("u8");
        case RecordObject::FLOAT:  return py::dtype("f4");
        case RecordObject::DOUBLE: return py::dtype("f8");
        case RecordObject::TIME8:  return py::dtype("i8");
        default:
            throw std::runtime_error(std::string("unsupported H5Coro datatype ") +
                                     std::to_string(static_cast<int>(ft)) + " for " + dataset);
    }
}

void free_info(H5Coro::info_t& info)
{
    if (info.data)
    {
        operator delete[](info.data, std::align_val_t(H5CORO_DATA_ALIGNMENT));
        info.data = NULL;
    }
}

using request_t = std::tuple<std::string, std::optional<int64_t>, std::optional<int64_t>>;

py::list read_datasets(const std::string& resource, const std::vector<request_t>& requests)
{
    std::string dir = ".";
    std::string base = resource;
    const auto pos = resource.rfind('/');
    if (pos != std::string::npos)
    {
        dir = resource.substr(0, pos);
        base = resource.substr(pos + 1);
    }

    Asset* asset = asset_for_dir(dir);
    H5Coro::Context context(asset, base.c_str());

    py::list out;
    for (const auto& [dataset, start, end] : requests)
    {
        H5Coro::range_t slice[1];
        int slicendims = 0;
        if (start.has_value() != end.has_value())
            throw std::runtime_error("start/end must both be set or both be None: " + dataset);
        if (start.has_value())
        {
            slice[0].r0 = *start;
            slice[0].r1 = *end;
            slicendims = 1;
        }

        H5Coro::info_t info;
        {
            py::gil_scoped_release release;
            info = H5Coro::read(&context, dataset.c_str(), RecordObject::DYNAMIC,
                                slicendims ? slice : NULL, slicendims);
        }

        try
        {
            std::vector<py::ssize_t> dims;
            for (int d = 0; d < H5Coro::MAX_NDIMS; d++)
                if (info.shape[d] > 0) dims.push_back(info.shape[d]);
            if (dims.empty()) dims.push_back(info.elements);
            py::ssize_t n = 1;
            for (const auto d : dims) n *= d;
            if (n != static_cast<py::ssize_t>(info.elements))
                throw std::runtime_error("shape/elements mismatch for " + dataset);

            py::array arr(np_dtype(info.datatype, dataset.c_str()), dims);
            std::memcpy(arr.mutable_data(),
                        info.data,
                        static_cast<size_t>(info.elements) * info.typesize);
            out.append(std::move(arr));
        }
        catch (...)
        {
            free_info(info);
            throw;
        }
        free_info(info);
    }
    return out;
}

py::dict footprint()
{
    py::dict d;
    Dl_info shim_info;
    if (dladdr(reinterpret_cast<void*>(&read_datasets), &shim_info) && shim_info.dli_fname)
        d["shim"] = std::string(shim_info.dli_fname);
    Dl_info lib_info;
    if (dladdr(reinterpret_cast<void*>(&H5Coro::deinit), &lib_info) && lib_info.dli_fname)
        d["libsliderule"] = std::string(lib_info.dli_fname);
    return d;
}

}  // namespace

PYBIND11_MODULE(h5shim, m)
{
    m.doc() = "benchmark shim over sliderule C++ H5Coro (zagg issue #149, phase 3)";
    m.attr("__version__") = "0.1.0";

    // one-time package init: initcore() registers the file IO driver and core
    // libs; inith5coro() starts the H5Coro reader pool (size pinned via the
    // H5CORO_THREAD_POOL_SIZE compile definition — see build.sh)
    initcore();
    inith5coro();

    m.def("read", &read_datasets, py::arg("resource"), py::arg("requests"),
          "read datasets from one granule; requests are (dataset, start, end) "
          "half-open row ranges on dim 0, or (dataset, None, None) for a full read");
    m.def("footprint", &footprint, "paths of the loaded shim and libsliderule shared objects");
}

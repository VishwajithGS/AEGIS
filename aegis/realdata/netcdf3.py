"""Minimal reader for classic NetCDF-3 files (CDF-1 / CDF-2), numpy only.

IMD's gridded rainfall files are classic NetCDF-3. The format is fully
specified (NetCDF Users Guide, "File Format Specification"); this reader
supports what those files use: fixed-size and record (unlimited) variables
of the numeric types, global and variable attributes. It avoids adding a
compiled NetCDF/HDF5 dependency to the Windows host and the Spark image.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Union

import numpy as np

_ABSENT = 0
_DIM, _VAR, _ATT = 0x0A, 0x0B, 0x0C
# nc_type -> (numpy big-endian dtype, size)
_TYPES = {1: (">i1", 1), 2: ("S1", 1), 3: (">i2", 2), 4: (">i4", 4), 5: (">f4", 4), 6: (">f8", 8)}


class NetCDF3Error(ValueError):
    pass


@dataclass
class Variable:
    name: str
    dims: tuple[str, ...]
    shape: tuple[int, ...]
    attrs: dict
    nc_type: int
    vsize: int
    begin: int
    is_record: bool


@dataclass
class Dataset:
    path: Path
    dims: dict[str, int]
    attrs: dict
    variables: dict[str, Variable] = field(default_factory=dict)
    numrecs: int = 0
    recsize: int = 0

    def read(self, name: str) -> np.ndarray:
        """Read a variable fully, applying _FillValue / missing_value as NaN for floats."""
        v = self.variables[name]
        dtype, size = _TYPES[v.nc_type]
        with open(self.path, "rb") as fh:
            if not v.is_record:
                fh.seek(v.begin)
                count = int(np.prod(v.shape)) if v.shape else 1
                arr = np.frombuffer(fh.read(count * size), dtype=dtype).reshape(v.shape)
            else:
                per_rec = int(np.prod(v.shape[1:])) if len(v.shape) > 1 else 1
                out = np.empty((self.numrecs, per_rec), dtype=dtype)
                for r in range(self.numrecs):
                    fh.seek(v.begin + r * self.recsize)
                    out[r] = np.frombuffer(fh.read(per_rec * size), dtype=dtype)
                arr = out.reshape((self.numrecs,) + tuple(v.shape[1:]))
        arr = arr.astype(arr.dtype.newbyteorder("="))
        if arr.dtype.kind == "f":
            arr = arr.astype("float64")
            for key in ("_FillValue", "missing_value"):
                if key in v.attrs:
                    for fill in np.atleast_1d(v.attrs[key]):
                        arr[np.isclose(arr, float(fill))] = np.nan
        return arr


def _read_name(fh: BinaryIO) -> str:
    n = struct.unpack(">i", fh.read(4))[0]
    raw = fh.read(n)
    fh.read((4 - n % 4) % 4)
    return raw.decode("utf-8", "replace")


def _read_values(fh: BinaryIO, nc_type: int, n: int):
    if nc_type not in _TYPES:
        raise NetCDF3Error(f"unsupported nc_type {nc_type}")
    dtype, size = _TYPES[nc_type]
    raw = fh.read(n * size)
    fh.read((4 - (n * size) % 4) % 4)
    if nc_type == 2:
        return raw.decode("utf-8", "replace").rstrip("\x00")
    vals = np.frombuffer(raw, dtype=dtype)
    return vals[0].item() if n == 1 else vals.astype(vals.dtype.newbyteorder("="))


def _read_atts(fh: BinaryIO) -> dict:
    tag, n = struct.unpack(">ii", fh.read(8))
    if tag == _ABSENT:
        return {}
    if tag != _ATT:
        raise NetCDF3Error(f"expected attribute list, got tag {tag:#x}")
    out = {}
    for _ in range(n):
        name = _read_name(fh)
        nc_type, nelems = struct.unpack(">ii", fh.read(8))
        out[name] = _read_values(fh, nc_type, nelems)
    return out


def open_dataset(path: Union[str, Path]) -> Dataset:
    path = Path(path)
    with open(path, "rb") as fh:
        magic = fh.read(4)
        if magic[:3] != b"CDF" or magic[3] not in (1, 2):
            raise NetCDF3Error(f"{path} is not a classic NetCDF-3 file (magic {magic!r})")
        offset_fmt = ">i" if magic[3] == 1 else ">q"
        numrecs = struct.unpack(">i", fh.read(4))[0]
        tag, n = struct.unpack(">ii", fh.read(8))
        dims_list: list[tuple[str, int]] = []
        if tag == _DIM:
            for _ in range(n):
                name = _read_name(fh)
                dims_list.append((name, struct.unpack(">i", fh.read(4))[0]))
        elif tag != _ABSENT:
            raise NetCDF3Error("bad dimension list")
        attrs = _read_atts(fh)
        ds = Dataset(path=path, dims={k: v for k, v in dims_list}, attrs=attrs, numrecs=numrecs)
        tag, n = struct.unpack(">ii", fh.read(8))
        if tag not in (_VAR, _ABSENT):
            raise NetCDF3Error("bad variable list")
        for _ in range(n if tag == _VAR else 0):
            name = _read_name(fh)
            ndims = struct.unpack(">i", fh.read(4))[0]
            dimids = struct.unpack(f">{ndims}i", fh.read(4 * ndims)) if ndims else ()
            vattrs = _read_atts(fh)
            nc_type, vsize = struct.unpack(">ii", fh.read(8))
            begin = struct.unpack(offset_fmt, fh.read(struct.calcsize(offset_fmt)))[0]
            dnames = tuple(dims_list[i][0] for i in dimids)
            shape = tuple(dims_list[i][1] if dims_list[i][1] != 0 else numrecs for i in dimids)
            is_record = bool(dimids) and dims_list[dimids[0]][1] == 0
            ds.variables[name] = Variable(name, dnames, shape, vattrs, nc_type, vsize, begin, is_record)
        ds.recsize = sum(v.vsize for v in ds.variables.values() if v.is_record)
        if sum(1 for v in ds.variables.values() if v.is_record) == 1:
            # single record variable: records are not padded (NetCDF spec special case)
            rv = next(v for v in ds.variables.values() if v.is_record)
            ds.recsize = int(np.prod(rv.shape[1:])) * _TYPES[rv.nc_type][1]
        return ds

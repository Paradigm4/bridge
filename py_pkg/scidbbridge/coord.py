import numpy
import pandas


# orig: chunk origin coordinates

def pos2coord(pos, orig, chunk_size):
    coord = []
    n_dims = len(orig)

    if n_dims == 1:
        coord.append(orig[0] + pos)                     # coord[0]
    elif n_dims == 2:
        coord.insert(0, orig[1] + pos % chunk_size[1])  # coord[1]
        pos //= chunk_size[1]
        coord.insert(0, orig[0] + pos)                  # coord[0]
    else:
        for i in range(n_dims - 1, -1, -1):
            coord.insert(0, orig[i] + pos % chunk_size[i])
            pos //= chunk_size[i]

    return coord


def pos2coord_all(data, pos_name, orig_names, chunk_size, res_dim_names):
    n_dims = len(orig_names)
    pos = data[pos_name]

    if n_dims == 1:
        res = (data[orig_names[0]] + pos, )
    if n_dims == 2:
        coord1 = data[orig_names[1]] + data[pos_name] % chunk_size[1]
        pos = pos // chunk_size[1]
        coord0 = data[orig_names[0]] + pos
        res = (coord0, coord1)
    else:
        res = []
        for i in range(n_dims - 1, -1, -1):
            res.insert(0, data[orig_names[i]] + pos % chunk_size[i])
            pos = pos // chunk_size[i]

    return pandas.DataFrame(dict(zip(res_dim_names, res)))



def coord2pos(coord, orig, chunk_size):
    n_dims = len(coord)

    if n_dims == 1:
        pos = coord[0] - orig[0]
    elif n_dims == 2:
        pos = ((coord[0] - orig[0]) * chunk_size[1] +
               (coord[1] - orig[1]))
    else:
        pos = 0
        for i in range(n_dims):
            pos *= chunk_size[i]
            pos += coord[i] - orig[i]

    return pos


def coord2pos_all(data, dim_names, orig_names, chunk_size):
    n_dims = len(dim_names)

    if n_dims == 1:
        res = data[dim_names[0]] - data[orig_names[0]]
    elif n_dims == 2:
        res = ((data[dim_names[0]] - data[orig_names[0]]) * chunk_size[1] +
               data[dim_names[1]] - data[orig_names[1]])
    else:
        res = pandas.Series(numpy.zeros(len(data)), dtype=numpy.int64)
        for i in range(n_dims):
            res *= chunk_size[i]
            res += data[dim_names[i]] - data[orig_names[i]]

    return res


def prev_delta2coord(prev_coord, delta, chunk_sizes):
    """
    >>> prev_delta2coord((1, 1, 0), 4, (4, 3, 2))
    [2, 0, 0]
    """
    coord = list(prev_coord)
    i = len(prev_coord) - 1
    while coord[i] + delta >= chunk_sizes[i]:
        coord[i] += delta
        delta = coord[i] // chunk_sizes[i]
        coord[i] %= chunk_sizes[i]
        i -= 1
    coord[i] += delta
    return coord


# util/ArrayCoordinatesMapper.h
#
# /**
#  * Convert logical chunk position (in row-major order)  to array coordinates
#  */
# void pos2coord(position_t pos,CoordinateRange coord) const
# {
#     assert(pos >= 0);
#     assert(coord.size() == _nDims);
#     if (_nDims == 1) {
#         coord[0] = _origin[0] + pos;
#         assert(pos < _chunkIntervals[0]);
#     } else if (_nDims == 2) {
#         coord[1] = _origin[1] + (pos % _chunkIntervals[1]);
#         pos /= _chunkIntervals[1];
#         coord[0] = _origin[0] + pos;
#         assert(pos < _chunkIntervals[0]);
#     } else {
#         for (int i=safe_static_cast<int>(_nDims); --i>=0;) {
#             coord[i] = _origin[i] + (pos % _chunkIntervals[i]);
#             pos /= _chunkIntervals[i];
#         }
#         assert(pos == 0);
#     }
# }
#
# /**
#  * Convert array coordinates to the logical chunk position (in row-major
#  * order)
#  */
# position_t coord2pos(CoordinateCRange coord) const
# {
#     assert(coord.size() == _nDims);
#     position_t pos(-1);
#     if (_nDims == 1) {
#         pos = coord[0] - _origin[0];
#         assert(pos < _chunkIntervals[0]);
#     } else if (_nDims == 2) {
#         pos = (coord[0] - _origin[0])*_chunkIntervals[1] +
#             (coord[1] - _origin[1]);
#     } else {
#         pos = 0;
#         for (size_t i = 0, n = _nDims; i < n; i++) {
#             pos *= _chunkIntervals[i];
#             pos += coord[i] - _origin[i];
#         }
#     }
#     assert(pos >= 0 && static_cast<uint64_t>(pos)<_logicalChunkSize);
#     return pos;
# }

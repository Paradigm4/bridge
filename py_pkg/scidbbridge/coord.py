def pos2coord(pos, origin, chunkSize):
    coord = []
    nDims = len(origin)

    if nDims == 1:
        coord.append(origin[0] + pos)  # coord[0]
    elif nDims == 2:
        coord.insert(0, origin[1] + pos % chunkSize[1])  # coord[1]
        pos //= chunkSize[1]
        coord.insert(0, origin[0] + pos)                 # coord[0]
    else:
        for i in range(nDims - 1, -1, -1):
            coord.insert(0, origin[i] + pos % chunkSize[i])
            pos //= chunkSize[i]

    return coord


def coord2pos(coord, origin, chunkSize):
    nDims = len(origin)

    if nDims == 1:
        pos = coord[0] - origin[0]
    elif nDims == 2:
        pos = ((coord[0] - origin[0]) * chunkSize[1] +
               (coord[1] - origin[1]))
    else:
        pos = 0
        for i in range(nDims):
            pos *= chunkSize[i]
            pos += coord[i] - origin[i]

    return pos


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

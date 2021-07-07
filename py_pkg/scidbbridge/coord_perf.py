import numpy
import pandas
import sys
import timeit

from coord import coord2pos, pos2coord

# iquery --afl --format csv+ --query "
#   project(
#     apply(
#       bernoulli(
#         project(ASSOC_RIVAS_RESULTS, nobs),
#         .002),
#       variant_id_chunk, variant_id / 100000,
#       sub_field_id_chunk, sub_field_id / 10),
#     variant_id_chunk, sub_field_id_chunk)
#  " > assoc_dims.1m.csv


# ---
# Setup
# ---
file_name_in = sys.argv[1]
chunkSize = (100000, 10)
names = ('variant_id',
         'sub_field_id',
         'variant_id_chunk',
         'sub_field_id_chunk')
if '_pos.' in file_name_in:
    names = names + ('pos', )
dtype = dict((k, numpy.int64) for k in names)


# ---
# Read input file
# ---
data = pandas.read_csv(file_name_in, names=names, dtype=dtype)


# ---
# Add pos column (run once)
# ---
file_name_out = file_name_in[:-4] + '_pos' + file_name_in[-4:]
data['pos'] = data.apply(lambda row: coord2pos(row[:2], row[2:], chunkSize),
                         axis=1)
data.to_csv(file_name_out, header=False, index=False)


# ---
# coord2pos
# ---
# data.apply(lambda row: coord2pos(row[:2], row[2:4], chunkSize), axis=1)


# ---
# pos2coord
# ---
# data.apply(lambda row: pos2coord(row[4], row[2:4], chunkSize), axis=1)


# ---
# timeit
# ---
# stmt = 'data.apply(lambda row: coord2pos(row[:2], row[2:4], chunkSize), \
# axis=1)'
# stmt = 'data.apply(lambda row: pos2coord(row[4], row[2:4], chunkSize), \
# axis=1)'
# res = timeit.timeit(
#     setup='data = pandas.read_csv(file_name_in, names=names, dtype=dtype)',
#     stmt=stmt,
#     number=10,
#     globals=globals())
# print(res)

# %timeit data.apply(lambda r: coord2pos(r[:2], r[2:4], chunkSize), axis=1)
# %timeit data.apply(lambda r: pos2coord(r[4], r[2:4], chunkSize), axis=1)

import numpy
import pandas
import sys
import timeit

from coord import coord2pos, pos2coord

# iquery --afl --format csv --query "
#   sort(
#     project(
#       apply(
#         limit(
#           project(ASSOC_RIVAS_RESULTS, nobs),
#           1000000),
#         d0,   variant_id,
#         d1,   sub_field_id,
#         d0_c, variant_id / 100000 * 100000,
#         d1_c, sub_field_id / 10 * 10),
#       d0, d1, d0_c, d1_c),
#     d0_c, d1_c, d0, d1)
#   " > assoc_dims.1m.csv


# ---
# Setup
# ---
file_name_in = sys.argv[1]
chunkSize = (100000, 10)
names = ('d0',                  # 0
         'd1',                  # 1
         'd0_c',                # 2
         'd1_c')                # 3
if '_pos.' in file_name_in:
    names = names + ('pos',
                     'prev_d0_c',
                     'prev_d1_c',
                     'prev_pos',
                     'delta')
dtype = dict((k, numpy.int64) for k in names)


# ---
# Read input file
# ---
data = pandas.read_csv(file_name_in, names=names, dtype=dtype)


# ---
# Add pos column (run once)
# ---
if '_pos.' not in file_name_in:
    file_name_out = file_name_in[:-4] + '_pos' + file_name_in[-4:]

    # Compute pos
    data['pos'] = data.apply(
        lambda row: coord2pos(row[:2], row[2:4], chunkSize),
        axis=1)                            # 4

    # Shift and assign prev_* columns
    data_prev = data.shift(1, fill_value=0)
    data['prev_d0_c'] = data_prev['d0_c']  # 5
    data['prev_d1_c'] = data_prev['d1_c']  # 6
    data['prev_pos'] = data_prev['pos']    # 7

    def pos2delta(row):
        if row[2:4].tolist() == row[5:7].tolist():  # Same chunk
            return row['pos'] - row['prev_pos']
        return row['pos']

    # Compute delta
    data['delta'] = data.apply(pos2delta, axis=1)

    # Reset first delta due to shift
    data.loc[0, 'delta'] = data.loc[0, 'pos']

    # Verify that no delta value is negative
    if any(data['delta'] < 0):
        raise Exception(
            "Something's off | Negative delta found\n{}".format(
                data.loc[data['delta'] < 0]))

    # Save to file
    data.to_csv(file_name_out, header=False, index=False)

    # Do reverse computation and verify results
    def delta2pos(row):
        if row[2:4].tolist() == row[5:7].tolist():  # Same chunk
            return row['prev_pos'] + row['delta']
        return row['delta']

    data['new_pos'] = data.apply(delta2pos, axis=1)
    data[['new_d0', 'new_d1']] = data.apply(
        lambda row: pos2coord(row['new_pos'], row[2:4], chunkSize),
        axis=1,
        result_type='expand')

    if not all(data['d0'] == data['new_d0']):
        raise Exception(
            "Something's off | Computed d0 coords don't match\n{}".format(
                data.loc[data['d0'] != data['new_d0']]))

    if not all(data['d1'] == data['new_d1']):
        raise Exception(
            "Something's off | Computed d1 coords don't match\n{}".format(
                data.loc[data['d1'] != data['new_d1']]))


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
# %timeit data.apply(lambda r: pos2coord(r[4], r[2:4], chunkSize), axis=1)650

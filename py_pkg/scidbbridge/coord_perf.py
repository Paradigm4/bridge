import itertools
import numpy
import pandas
import pyarrow
import sys

from coord import *

# import timeit


# iquery --afl --format csv --query "
#   sort(
#     project(
#       apply(
#         limit(
#           project(ASSOC_RIVAS_RESULTS, nobs),
#           1000000),
#         d0,   variant_id,
#         d1,   sub_field_id,
#         d0_o, variant_id / 100000 * 100000,
#         d1_o, sub_field_id / 10 * 10),
#       d0, d1, d0_o, d1_o),
#     d0, d1)
#   " > assoc_dims.1m.csv


# ---
# Read input file
# ---
def read_file(file_name, n_dims, attr_names=(), attr_types=()):
    dim_names = tuple('d{}'.format(i) for i in range(n_dims))
    dim_dtype = dict((k, numpy.int64) for k in dim_names)

    names = tuple(itertools.chain(dim_names, attr_names))
    dtypes = dict(itertools.chain(dim_dtype.items(),
                                  ((name, dtype) 
                                   for (name, dtype) in zip(attr_names,
                                                            attr_types))))

    return pandas.read_csv(file_name, names=names, dtype=dtypes)


# ---
# Write output file
# ---
def write_dile(data, file_name):
    data.to_csv(file_name, header=False, index=False)


# ---
# Add pos column
# ---
def compute_delta(data, origins, chunk_sizes):
    n_dims = len(origins)
    n_atts = len(data.columns) - n_dims
    
    dim_names = tuple('d{}'.format(i) for i in range(n_dims))
    origin_names = tuple('o{}'.format(i) for i in range(n_dims))
    prev_origin_names = tuple('prev_o{}'.format(i) for i in range(n_dims))
    new_dim_names = tuple('new_d{}'.format(i) for i in range(n_dims))

    # Compute chunk origin
    for i in range(n_dims):
        data[origin_names[i]] = ((data[dim_names[i]] - origins[i]) 
                                 // chunk_sizes[i]
                                 * chunk_sizes[i]
                                 + origins[i])

    # Compute pos
    data['pos'] = coord2pos_all(data,
                                dim_names,
                                origin_names,
                                chunk_sizes)

    # Shift and assign prev_* columns
    data_prev = data.shift(1, fill_value=0)

    # Set previous values (for delta compuation)
    for i in range(n_dims):
        data[prev_origin_names[i]] = data_prev[origin_names[i]]
    data['prev_pos'] = data_prev['pos']

    # Set new chunk markers
    data['new_chunk'] = False
    for (name, prev_name) in zip(origin_names, prev_origin_names):
        data['new_chunk'] |= (data[name] != data[prev_name])

    # if new_chunk then delta = pos
    # else delta = pos - prev_pos
    data['delta'] = data['pos'] - ~data['new_chunk'] * data['prev_pos']

    # Reset first delta due to shift
    data.loc[0, 'delta'] = data.loc[0, 'pos']

    # Verify that no delta value is negative
    if any(data['delta'] < 0):
        raise Exception(
            "Something's off | Negative delta found\n{}".format(
                data.loc[data['delta'] < 0]))

    # Compute new pos
    # if new_chunk pos = delta
    # else pos = prev_pos + delta
    data['new_pos'] = ~data['new_chunk'] * data['prev_pos'] + data['delta']

    # Compute new coords
    data = data.join(
        pos2coord_all(data, 
                      'new_pos',
                      origin_names,
                      chunk_sizes,
                      new_dim_names))

    # Verify that new pos and coords match
    for (name, new_name) in zip(dim_names, new_dim_names):
        if not all(data[name] == data[new_name]):
            raise Exception(
                ("Something's off | Computed {} " +
                 "coords don't match\n{}").format(
                     name,
                     data.loc[data[name] != data[new_name]]))
        if not all(data[name] == data[new_name]):
            raise Exception(
                ("Something's off | Computed {} " +
                 "coords don't match\n{}").format(
                     name,
                     data.loc[data[name] != data[new_name]]))

    # Delete new pos and coords
    del data['new_pos']
    for name in new_dim_names:
        del data[name]


# ---
# Write Arrow table
# ---
def write_arrow(file_name, dataframe, compression='lz4'):
    table = pyarrow.Table.from_pandas(dataframe)
    table = table.replace_schema_metadata()
    stream = pyarrow.output_stream(file_name, compression=compression)
    writer = pyarrow.ipc.RecordBatchStreamWriter(stream, table.schema)
    writer.write_table(table)
    writer.close()
    stream.close()


# ---
# timeit
# ---

# %timeit data.apply(lambda r: coord2pos(r[:2], r[2:4], chunk_size), axis=1)
# %timeit data.apply(lambda r: pos2coord(r[4], r[2:4], chunk_size), axis=1)

# %timeit coord2pos_all(data, ('d0', 'd1'), ('d0_o', 'd1_o'), chunk_size)
# %timeit pos2coord_all(
#     data, 'pos', ('d0_o', 'd1_o'), chunk_size, ('d0', 'd1'))

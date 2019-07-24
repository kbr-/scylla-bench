#!/usr/bin/python3

from cassandra.cluster import Cluster
import sys
import shutil
import os
import time
import subprocess

def read_chunks(f, sz):
    while True:
        data = f.read(sz)
        if not data:
            break
        yield data


def du(path):
    return subprocess.check_output(['du','-sh', path]).split()[0].decode('utf-8')

def mk_compr_dict(compr, level, chunk):
    return "{{{}}}".format(", ".join(
        ["'sstable_compression': '{}'".format(compr)] +
        ([] if level is None else ["'compression_level': '{}'".format(level)]) +
        ([] if chunk is None else ["'chunk_length_in_kb': '{}'".format(chunk)])))

NAMESPACE_PREFIX = 'org.apache.cassandra.io.compress.'
LZ4 = ('LZ4', 'LZ4Compressor')
SNAPPY = ('Snappy', 'SnappyCompressor')
DEFLATE = ('Deflate', 'DeflateCompressor')
ZSTD = ('Zstd', 'ZstdCompressor')
ZSTD_LEVELS = list(range(1, 23, 4))
COMPR_CHUNKS = [4, 16, 64]

# [(short name, long name, level)]
COMPRESSIONS = ([('No compression', '', None)]
    + [(c[0], NAMESPACE_PREFIX + c[1], None) for c in [LZ4, SNAPPY, DEFLATE]]
    + [('{} {}'.format(ZSTD[0], l), NAMESPACE_PREFIX + ZSTD[1], l) for l in ZSTD_LEVELS]
)

def compressions(chunk):
    return [(cname, mk_compr_dict(clong, level, chunk)) for cname, clong, level in COMPRESSIONS]

SCYLLA_HOME = '/home/kbraun/dev/scylla/'

HOW_MUCH = 30 * 1024 * 1024
CHUNK_SIZE = 1024

KS_DIR = os.path.join(SCYLLA_HOME, 'tmp/test_ks')

# Takes a file name, compression dict, returns (time, space)
def bench_compr(fname, cdict):
    with Cluster() as cluster:
        session = cluster.connect('test_ks')

        session.execute("drop table if exists test_struct")
        if os.path.exists(KS_DIR):
            shutil.rmtree(KS_DIR)

        session.execute("create table test_struct (a int, b int, c text, primary key (a, b)) with compression = {}".format(cdict))

        num_partitions = 10
        ixs = [0]*num_partitions

        with open(fname, 'r', encoding='latin1') as f:
            much = 0
            currp = 0
            for c in read_chunks(f, CHUNK_SIZE):
                session.execute('insert into test_struct (a, b, c) values (%s, %s, %s)', (currp, ixs[currp], c))

                much += CHUNK_SIZE
                if much >= HOW_MUCH:
                    break

                sys.stdout.write('\rInserting data: %d%%...' % int(much / HOW_MUCH * 100))
                sys.stdout.flush()

                ixs[currp] += 1
                currp = (currp + 1) % num_partitions

        sys.stdout.write('\n')
        sys.stdout.flush()

        print("Flushing...")
        t1 = time.time()
        os.system("nodetool flush")
        t2 = time.time()

        duration = (t2 - t1) * 1000.
        space = du(KS_DIR)
        print("Done.\nTime: {:.3f} ms\nSpace: {}".format(duration, space))
        return (duration, space)

if len(sys.argv) < 2:
    print("Requires data file name")
    sys.exit(0)

fname = sys.argv[1]
if not os.path.exists(fname):
    print("File does not exist:", fname)
    sys.exit(0)

results = []
for chunk in COMPR_CHUNKS:
    print("Compression chunk length in kb:", chunk)
    res_chunk = []
    for cname, cdict in compressions(chunk):
        print("Benchmarking compression:", cname)
        res_chunk.append((cname, bench_compr(fname, cdict)))
    results.append((chunk, res_chunk))

print("results:\n", results)

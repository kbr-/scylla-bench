#!/usr/bin/python3

from cassandra.cluster import Cluster
import sys
import shutil
import os
import time
import subprocess
import json
import re
import statistics as stats

def read_chunks(f, sz):
    while True:
        data = f.read(sz)
        if not data:
            break
        yield data


def du(path):
    return subprocess.check_output(['du','-s', path]).split()[0].decode('utf-8')

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
ZSTD_LEVELS = [1, 3, 5, 7, 9]
COMPR_CHUNKS = [4, 16, 64]

# [(short name, long name, level)]
COMPRESSIONS = ([('None', '', None)]
    + [(c[0], NAMESPACE_PREFIX + c[1], None) for c in [LZ4, SNAPPY, DEFLATE]]
    + [('{} {}'.format(ZSTD[0], l), NAMESPACE_PREFIX + ZSTD[1], l) for l in ZSTD_LEVELS]
)

def compressions(chunk):
    return [(cname, mk_compr_dict(clong, level, chunk)) for cname, clong, level in COMPRESSIONS]

SCYLLA_HOME = '/home/kbraun/dev/scylla/'

HOW_MUCH = 10 * 1024 * 1024
CHUNK_SIZE = 1024

KS_DIR = os.path.join(SCYLLA_HOME, 'tmp/test_ks')

if len(sys.argv) < 4:
    print("Requires data file name, output file name, Scylla's stderr file name")
    sys.exit(0)

infname = sys.argv[1]
outfname = sys.argv[2]
logfname = sys.argv[3]

for fname in [infname, outfname, logfname]:
    if not os.path.exists(fname):
        print("File does not exist:", fname)
        sys.exit(0)

def consume_stalls(f):
    res = []
    while True:
        line = f.readline()
        if not line:
            return res
        ms = re.match(r"Reactor stalled for (\d+) ms.*", line)
        if ms:
            res.append(int(ms.group(1)))

# Takes a file name, compression dict, handle to file with Scylla's stderr, returns (time, space, stalls)
def bench_compr(fname, cdict, logf):
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
        logf.seek(0, 2)
        t1 = time.time()
        os.system("nodetool flush")
        t2 = time.time()
        stalls = consume_stalls(logf)
        print("Done.")

        duration = (t2 - t1) * 1000.
        space = du(KS_DIR)
        return (duration, int(space), stalls)

results = []
with open(logfname, 'r') as logf:
    logf.seek(0, 2)
    for chunk in COMPR_CHUNKS:
        print("Compression chunk length in kb:", chunk)
        res_chunk = []
        for cname, cdict in compressions(chunk):
            print("Benchmarking compression:", cname)
            t, space, stalls = bench_compr(infname, cdict, logf)

            mean_stall = stats.mean(stalls) if stalls else 0
            stdev_stall = stats.stdev(stalls) if len(stalls) > 1 else 0

            res_chunk.append({'name': cname, 'flush_time': t, 'space': space, 'stalls': len(stalls), 'mean_stall': mean_stall, 'stall_stdev': stdev_stall})

            print("Time: {:.3f} ms\nSpace: {}\nReactor stalls: {}, stall mean: {} ms, stall stdev: {} ms".format(t, space, len(stalls), mean_stall, stdev_stall))
        results.append({'chunk_len': chunk, 'results': res_chunk})

print("results:\n", results)
with open(outfname, 'w') as outf:
    json.dump(results, outf)

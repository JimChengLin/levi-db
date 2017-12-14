# LeviDB8: A Fast and Persistent Key-Value Storage Engine
[![Build Status](https://travis-ci.org/JimChengLin/levi-db.svg?branch=master)](https://travis-ci.org/JimChengLin/levi-db)
[![Coverage Status](https://coveralls.io/repos/github/JimChengLin/levi-db/badge.svg?branch=master)](https://coveralls.io/github/JimChengLin/levi-db?branch=master)
<a href="https://scan.coverity.com/projects/jimchenglin-levi-db">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/13175/badge.svg"/>
</a>

In Computer Science, key-value mapping is a fundamental and common scenario. For in-memory usage, there is Redis. If persistence
is concerned, we also have a batch of options, e.g. LevelDB, RocksDB... It is oblivious that there must be a
trade-off between persistence and speed. Computers may crash at anytime due to software design flaws, CPU overheat, etc..
Therefore, the persistent storage engines have to write backup data into hard disk or SSD before the operation is sensible for the
 outside world, then if something goes wrong, the engine could read the log and recovery. The log is called WAL(Write Ahead Log) academically.
 However, does anyone wonder why each piece of data will be written at least twice? One for WAL, one for the real data structure like
 B-Tree, B+Tree, LSMT, etc..

 The author of LeviDB8 has invented a new data structure named BitDegradeTree(BDT) that is kind of combination of CritBitTree and B-Tree.
 It is **EXTREME FAST**! For BDT, WAL is data and data is WAL. The only extra space cost is a tiny index. Each record costs exact
 32Bits constantly. Normally, the index is one hundred times smaller than the data(WAL). It is acceptable to put the whole index into virtual memory(mmap).
 Not only that, but several other special optimisations including SSE4.2, fine-grained lock have deployed.

# Benchmark
Data Set: [Amazon Movies Review](https://snap.stanford.edu/data/web-Movies.html)

Operations: Write 100,000 records, Read 100,000 records, Iterate 100,000 records. Record size is 1KB roughly.

Data Size: 100MB

Environment: Ubuntu 16.04 LTS, Intel i7-4710HQ, 16GB DDR3, WDC 5400RPM 1TB with 8GB SSD

    Write 100K
    LeviDB8:  680 ms
    RocksDB: 2750 ms

    Read  100K
    LeviDB8:  325 ms
    RocksDB: 2595 ms

    Scan  100K
    LeviDB8: 188 ms
    RocksDB: 215 ms

As we can see above, LeviDB8 is 3 times faster than RocksDB for write, 7 times for read, is even for scan. The author didn't expect LeviDB8 could
be that fast either. The benchmark is pretty fair, or maybe you can find mistakes. In doubt and wanna see the benchmark source code?
Check "[bench/db_bench.cpp](https://github.com/JimChengLin/levi-db/blob/master/bench/db_bench.cpp)". If you can read Chinese,
here is an [article](https://zhuanlan.zhihu.com/p/31986751) explaining how to bench, or you can concat me via GitHub or Gmail(jimzuolin@gmail.com).

# Limitations
I know BDT and LeviDB8 is crazy! It does have a few limitations.
- The max data file is 2GB
- The data file manager will not reclaim the deleted resources, the index will recycle though
- If you run multiple threads to write and read concurrently, everything is expected like normal mul-threads programming.
if you write data meanwhile there is a in-progressing scan, ops!

What will be the right expectation for Limitation 3? Anyway, it would not be the end of the world.
I have considered the case carefully. The scanner will just reflect all changes in time, BUT may go back a little bit, i.e. the output order could be
1 => 2 => 3 => 2 => 3 => 4. Although it is not so beautiful, it is guaranteed that you will never have endless loop.
It is quite hard to understand why it happens. I think no one actually implements a concurrent trie that support iteration before.

All those limitations I mentioned above could be solved in future releases. I plan to write an aggregator object that manage(merge)
a batch of LeviDB8 instances. If an instances goes full, then the aggregator would split(compact) it into two, and the wasted spaces would be
freed also. Compaction could increase the WAF. I assume LeviDB8 would still be very competitive.

# Requirements
- Compiler that support C++14 required, GCC 6.0 is tested and recommended
- CPU that supports SSE4.2, most modern(after 2007) X86 CPU do
- Zstd installed
- JeMalloc installed
- CMake 3.8 is tested and recommended. You can try to modify CMakeLists.txt in order to use an older one.

# How to Compile
- `mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release`
- `make levidb8 -j2` # if you want to test
- `make levidb8-static` # if you want static library
- `make levidb8-shared` # if you want shared library

# Usage
Firstly, you can look at files under `include` folder. The main interface is `db.h`. The API is mostly stolen from LevelDB. People who
used LevelDB or RocksDB before would feel quite familiar. You can also check out tests under `test` folder. At last,
if nothing could help you, always contact me.

All methods if are not marked `noexcept` may throw `levidb8::Exception`, please catch or log them somewhere!

## Opening A database
```c++
levidb8::OpenOptions options{};
options.create_if_missing = true;
std::shared_ptr<levidb8::db> db = levidb8::DB::open("/tmp/testdb", options);
```

If you want to raise an error if the database already exists, add the following line before the `levidb8::DB::open` call:
```c++
options.error_if_exist = true;
```

## Reads And Writes
**The max size of key is 2^13(8192)bytes**. The max size of value is 2^32bytes.
```c++
std::string k = "k";
std::string v = "v";
db->put(k, v, levidb8::PutOptions{});
std::pair<std::string, bool/* success */> res = db->get(k);
assert(res.first == v && res.second);
```

## Atomic Updates
```c++
std::pair<levidb8::Slice, levidb8::Slice>
commands[2] = {{"1", "1"}, // add key 1 with value 1
               {"2", levidb8::nullSlice()}}; // delete the item whose key is 2
db->write(commands, 2, levidb8::WriteOptions{});
```

## Synchronous Writes
```c++
levidb8::PutOptions options{};
options.sync = true;
db->put(k, v, options);
```

## Concurrency
Within a single process, the same `levidb8::DB` object may be safely shared by
multiple concurrent threads, i.e. different threads may write into or fetch
iterators or call get on the same database without any external synchronization(the LeviDB8
implementation will automatically do the required synchronization).
However other objects(like Iterator) require external synchronization.

## Iteration
```c++
auto it = db->scan(); // shared_ptr, don't delete
for (it->seekToFirst(); it->valid(); it->next()) {
  cout << it->key().toString() << ": "  << it->value().toString() << endl;
}
```

# Thanks and My Wishes
I reuse a lot of code from LevelDB, thanks for sharing knowledge freely! The author currently live in Beijing, China
and will be free from March 2018 to September 2018. I am looking for a part time jobs or interns. Employers or HR, email me :)!

LeviDB8 is released under AGPL license. If you follow the rule, you can do whatever you want. Otherwise, for example you want to use LeviDB8 for
non-open-sourced business project, please considering give me a fair payment.
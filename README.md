# levi-db
[![Build Status](https://travis-ci.org/JimChengLin/levi-db.svg?branch=master)](https://travis-ci.org/JimChengLin/levi-db)
[![Coverage Status](https://coveralls.io/repos/github/JimChengLin/levi-db/badge.svg?branch=master)](https://coveralls.io/github/JimChengLin/levi-db?branch=master)
<a href="https://scan.coverity.com/projects/jimchenglin-levi-db">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/13175/badge.svg"/>
</a>

### Task List:
- [ ] URGENCY: basic compaction
- [ ] Repair tools
- [ ] Improve \[index iterator seek, AddInternal\] algorithm
- [ ] May \[add, del\] when iterate(in-memory snapshot)
- [ ] May sync when \[add, del\]
- [ ] Richer operation info
- [ ] Use entropy encoder
- [ ] Safer exception handle, e.g. StoreIterator
- [ ] Smarter compaction
- [ ] Transation support
- [ ] Persistent snapshot(backup)

### Thanks:
- LevelDB
- RocksDB
- Divsufsort
- CritBitTree
- People who shares knowledge freely
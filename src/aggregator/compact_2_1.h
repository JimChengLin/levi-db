#ifndef LEVIDB_TWO_TO_ONE_COMPACT_H
#define LEVIDB_TWO_TO_ONE_COMPACT_H

/*
 * 将两个数据库分片 compact 成一个
 * 因为工作比较简单, 不采用实时替换的 compact 方案
 */

#include "../db_single.h"

namespace LeviDB {
    class Compacting2To1Worker {
    private:
        std::unique_ptr<DB> _resource_a;
        std::unique_ptr<DB> _resource_b;
        std::unique_ptr<DB> _product;
        std::exception_ptr _e_a;
        std::exception_ptr _e_b;

    public:
        Compacting2To1Worker(std::unique_ptr<DB> && resource_a, std::unique_ptr<DB> && resource_b,
                             SeqGenerator * seq_gen);

        EXPOSE(_product);
    };
}

#endif //LEVIDB_TWO_TO_ONE_COMPACT_H
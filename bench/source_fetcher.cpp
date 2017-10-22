#ifdef LEVI_BENCH

#include "source_fetcher.h"

std::pair<std::string, std::string>
SourceFetcher::readItem() {
    for (int i = 0; i < 9; ++i) {
        _que[i] = _src.readLine();
    }
    return {{_que[0] + _que[1] + std::to_string(_nth++)},
            {_que[2] + _que[3] + _que[4] + _que[5] + _que[6] + _que[7]}};
}

#endif // LEVI_BENCH
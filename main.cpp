#include <iostream>

namespace levidb {
    namespace db_bench {
        void Run();
    }
}

int main() {
    levidb::db_bench::Run();
    std::cout << "Done." << std::endl;
    return 0;
}
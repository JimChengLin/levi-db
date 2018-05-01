#include <iostream>

namespace levidb {
    namespace db_test {
        void Run();
    }
    namespace db_bench {
        void Run();
    }
}

int main() {
    levidb::db_test::Run();
    levidb::db_bench::Run();
    std::cout << "Done." << std::endl;
    return 0;
}
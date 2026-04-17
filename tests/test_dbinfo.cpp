#include "database.hpp"
#include <cassert>
#include <cstdlib>

auto main() -> int {
    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->page_size() == 4096);
        assert(db->num_tables() == 3);
    }

    {
        auto db = Database::open("nonexistent.db");
        assert(!db.has_value());
    }

    return EXIT_SUCCESS;
}

#include "database.hpp"
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <sstream>
#include <string>

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

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        auto names = db->table_names();
        assert(names.size() == 2);
        assert(std::ranges::equal(names, std::array{"apples", "oranges"}));
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->rootpage("apples") == 2);
        assert(db->rootpage("oranges") == 4);
        assert(db->rootpage("sqlite_sequence") == 3);
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        assert(db->count_rows(2) == 4);
    }

    {
        auto db = Database::open("sample.db");
        assert(db.has_value());
        std::ostringstream out;
        handle_command(*db, "SELECT COUNT(*) FROM apples", out);
        assert(out.str() == "4\n");
    }

    return EXIT_SUCCESS;
}

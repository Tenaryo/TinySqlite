#pragma once

#include "util.hpp"

#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct WhereClause {
    std::string_view column;
    std::string_view value;
};

struct ColumnFilter {
    size_t column_index;
    std::string_view value;
};

struct SelectStatement {
    std::vector<std::string_view> columns;
    std::string_view table;
    bool is_count = false;
    std::optional<WhereClause> where;
};

inline auto parse_select(std::string_view sql) -> std::expected<SelectStatement, std::string> {
    static constexpr size_t kSelectKeywordLen = 6;
    static constexpr size_t kFromKeywordLen = 4;
    static constexpr size_t kWhereKeywordLen = 5;

    auto from_pos = util::find_ci(sql, "FROM");
    if (from_pos == std::string_view::npos)
        return std::unexpected("Invalid SELECT: missing FROM");

    auto after_select = sql.find_first_not_of(' ', kSelectKeywordLen);
    auto col_region = sql.substr(after_select, from_pos - after_select);
    while (!col_region.empty() && col_region.back() == ' ')
        col_region.remove_suffix(1);

    bool is_count = util::find_ci(col_region, "COUNT(*)") != std::string_view::npos;

    std::vector<std::string_view> columns;
    if (!is_count) {
        size_t pos = 0;
        while (pos < col_region.size()) {
            auto comma = col_region.find(',', pos);
            auto end = comma == std::string_view::npos ? col_region.size() : comma;
            auto col = col_region.substr(pos, end - pos);
            while (!col.empty() && col.front() == ' ')
                col.remove_prefix(1);
            while (!col.empty() && col.back() == ' ')
                col.remove_suffix(1);
            columns.push_back(col);
            pos = end + 1;
        }
    }

    auto tbl_start = sql.find_first_not_of(' ', from_pos + kFromKeywordLen);
    auto remaining = sql.substr(tbl_start);

    std::optional<WhereClause> where;
    auto where_pos = util::find_ci(remaining, "WHERE");
    std::string_view table_name;
    if (where_pos != std::string_view::npos) {
        table_name = remaining.substr(0, where_pos);
        while (!table_name.empty() && table_name.back() == ' ')
            table_name.remove_suffix(1);

        auto after_where = remaining.find_first_not_of(' ', where_pos + kWhereKeywordLen);
        auto condition = remaining.substr(after_where);

        auto eq_pos = condition.find('=');
        if (eq_pos != std::string_view::npos) {
            auto col = condition.substr(0, eq_pos);
            while (!col.empty() && col.back() == ' ')
                col.remove_suffix(1);

            auto val = condition.substr(eq_pos + 1);
            while (!val.empty() && val.front() == ' ')
                val.remove_prefix(1);
            while (!val.empty() && val.back() == ' ')
                val.remove_suffix(1);
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'')
                val = val.substr(1, val.size() - 2);

            where = WhereClause{.column = col, .value = val};
        }
    } else {
        table_name = remaining;
    }

    return SelectStatement{
        .columns = std::move(columns),
        .table = table_name,
        .is_count = is_count,
        .where = std::move(where),
    };
}

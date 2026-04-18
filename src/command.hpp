#pragma once

#include "database.hpp"
#include "sql_parser.hpp"

#include <cstdint>
#include <format>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

struct QueryResult {
    std::vector<std::vector<std::string>> rows;
};

inline auto format_rows(const QueryResult& result, std::ostream& out) -> void {
    for (const auto& row : result.rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0)
                out << '|';
            out << row[i];
        }
        out << '\n';
    }
}

inline auto execute_query(const Database& db,
                          const SelectStatement& sel) -> std::expected<QueryResult, std::string> {
    auto rp = db.rootpage(sel.table);
    if (!rp)
        return std::unexpected(rp.error());

    if (sel.is_count) {
        return QueryResult{.rows = {{{std::to_string(db.count_rows(*rp))}}}};
    }

    auto sql = db.table_sql(sel.table);
    auto table_cols = db.parse_create_table(sql);

    std::vector<size_t> col_indices;
    col_indices.reserve(sel.columns.size());
    for (const auto& col_name : sel.columns) {
        for (size_t i = 0; i < table_cols.size(); ++i) {
            if (util::iequals(table_cols[i], col_name)) {
                col_indices.push_back(i);
                break;
            }
        }
    }

    ColumnFilter filter_storage{0, {}};
    const ColumnFilter* filter = nullptr;
    if (sel.where) {
        for (size_t i = 0; i < table_cols.size(); ++i) {
            if (util::iequals(table_cols[i], sel.where->column)) {
                filter_storage.column_index = i;
                filter_storage.value = sel.where->value;
                filter = &filter_storage;
                break;
            }
        }
    }

    if (sel.where) {
        auto idx_rp = db.index_rootpage(sel.table, sel.where->column);
        if (idx_rp) {
            std::vector<uint64_t> rowids;
            db.index_search(*idx_rp, sel.where->value, rowids);
            std::ranges::sort(rowids);
            QueryResult result;
            for (auto rid : rowids) {
                auto row = db.read_row_by_rowid(*rp, rid, col_indices);
                if (row)
                    result.rows.push_back(std::move(*row));
            }
            return result;
        }
    }

    return QueryResult{.rows = db.read_columns_values(*rp, col_indices, filter)};
}

inline auto
handle_command(const Database& db, std::string_view command, std::ostream& out) -> void {
    if (command.starts_with('.')) {
        if (command == ".dbinfo") {
            out << "database page size: " << db.page_size() << '\n'
                << "number of tables: " << db.num_tables() << '\n';
        } else if (command == ".tables") {
            for (const auto& name : db.table_names())
                out << name << ' ';
            out << '\n';
        }
        return;
    }

    auto sel = parse_select(command);
    if (!sel) {
        out << sel.error() << '\n';
        return;
    }

    auto result = execute_query(db, *sel);
    if (!result) {
        out << result.error() << '\n';
        return;
    }

    format_rows(*result, out);
}

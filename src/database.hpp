#pragma once

#include "sql_parser.hpp"
#include "util.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

class Database {
    std::vector<std::byte> data_;
    uint32_t page_size_;
    uint32_t num_tables_;

    struct SchemaEntry {
        std::string_view type;
        std::string_view name;
        uint32_t rootpage;
        std::string_view sql;
    };

    std::vector<SchemaEntry> schema_;

    static constexpr size_t kPageSizeOffset = 16;
    static constexpr size_t kSchemaCountOffset = 103;
    static constexpr size_t kCellPtrArrayStart = 108;

    static constexpr uint8_t kTableInterior = 0x05;
    static constexpr uint8_t kTableLeaf = 0x0D;
    static constexpr uint8_t kIndexInterior = 0x02;
    static constexpr uint8_t kIndexLeaf = 0x0A;

    explicit Database(std::vector<std::byte> data)
        : data_(std::move(data)), page_size_(read_u16_be(kPageSizeOffset)),
          num_tables_(read_u16_be(kSchemaCountOffset)) {
        for (auto offset : cell_offsets())
            schema_.push_back(parse_schema_entry(offset));
    }

    auto read_u16_be(size_t offset) const noexcept -> uint16_t {
        assert(offset + 2 <= data_.size());
        return static_cast<uint16_t>(data_[offset]) << 8 | static_cast<uint16_t>(data_[offset + 1]);
    }

    auto read_u32_be(size_t offset) const noexcept -> uint32_t {
        assert(offset + 4 <= data_.size());
        return static_cast<uint32_t>(data_[offset]) << 24 |
               static_cast<uint32_t>(data_[offset + 1]) << 16 |
               static_cast<uint32_t>(data_[offset + 2]) << 8 |
               static_cast<uint32_t>(data_[offset + 3]);
    }

    struct VarintResult {
        uint64_t value;
        size_t consumed;
    };

    auto read_varint(size_t offset) const noexcept -> VarintResult {
        assert(offset + 1 <= data_.size());
        uint64_t value = 0;
        for (int i = 0; i < 9; ++i) {
            assert(offset + static_cast<size_t>(i) + 1 <= data_.size());
            auto byte = static_cast<uint8_t>(data_[offset + static_cast<size_t>(i)]);
            if (i == 8) {
                value = (value << 8) | byte;
                return {value, 9};
            }
            value = (value << 7) | static_cast<uint64_t>(byte & 0x7F);
            if ((byte & 0x80) == 0)
                return {value, static_cast<size_t>(i) + 1};
        }
        std::unreachable();
    }

    static constexpr auto serial_type_size(uint64_t serial_type) noexcept -> size_t {
        if (serial_type <= 4)
            return std::array{0, 1, 2, 3, 4}[serial_type];
        if (serial_type == 5)
            return 6;
        if (serial_type == 6 || serial_type == 7)
            return 8;
        if (serial_type >= 12 && serial_type % 2 == 0)
            return static_cast<size_t>((serial_type - 12) / 2);
        if (serial_type >= 13 && serial_type % 2 == 1)
            return static_cast<size_t>((serial_type - 13) / 2);
        return 0;
    }

    auto cell_offsets() const -> std::vector<uint16_t> {
        std::vector<uint16_t> offsets;
        offsets.reserve(num_tables_);
        for (uint32_t i = 0; i < num_tables_; ++i)
            offsets.push_back(read_u16_be(kCellPtrArrayStart + i * 2));
        return offsets;
    }

    auto parse_schema_entry(size_t cell_offset) const -> SchemaEntry {
        auto pos = cell_offset;
        pos += read_varint(pos).consumed;
        pos += read_varint(pos).consumed;

        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;

        static constexpr int kNumSchemaFields = 5;
        uint64_t serial_types[kNumSchemaFields];
        for (int i = 0; i < kNumSchemaFields; ++i) {
            auto vr = read_varint(pos);
            serial_types[i] = vr.value;
            pos += vr.consumed;
        }

        auto body = header_start + header_size_vr.value;
        size_t off = 0;

        auto read_text = [&](int idx) -> std::string_view {
            auto sz = serial_type_size(serial_types[idx]);
            auto sv =
                std::string_view{reinterpret_cast<const char*>(data_.data() + body + off), sz};
            off += sz;
            return sv;
        };

        auto read_uint = [&](int idx) -> uint32_t {
            auto sz = serial_type_size(serial_types[idx]);
            uint32_t val = 0;
            for (size_t j = 0; j < sz; ++j)
                val = (val << 8) | static_cast<uint8_t>(data_[body + off + j]);
            off += sz;
            return val;
        };

        auto type = read_text(0);
        auto name = read_text(1);
        read_text(2);
        auto rootpage = serial_types[3] == 0 ? 0u : read_uint(3);
        auto sql = serial_types[4] == 0 ? std::string_view{} : read_text(4);

        return {.type = type, .name = name, .rootpage = rootpage, .sql = sql};
    }

    auto parse_index_cell(size_t cell_offset) const -> std::pair<std::string_view, uint64_t> {
        auto pos = cell_offset;
        pos += read_varint(pos).consumed;

        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;

        auto st0 = read_varint(pos);
        pos += st0.consumed;
        auto st1 = read_varint(pos);
        pos += st1.consumed;

        auto body = header_start + header_size_vr.value;
        auto sz0 = serial_type_size(st0.value);
        auto col_val = std::string_view{reinterpret_cast<const char*>(data_.data() + body), sz0};

        auto sz1 = serial_type_size(st1.value);
        uint64_t rowid = 0;
        for (size_t j = 0; j < sz1; ++j)
            rowid = (rowid << 8) | static_cast<uint8_t>(data_[body + sz0 + j]);

        return {col_val, rowid};
    }

    auto resolve_cell_value(uint64_t serial_type,
                            size_t size,
                            uint64_t rowid,
                            size_t body_base,
                            size_t body_offset) const -> std::string {
        if (serial_type == 0)
            return std::to_string(rowid);
        if (serial_type == 8)
            return "0";
        if (serial_type == 9)
            return "1";
        if (serial_type >= 1 && serial_type <= 6) {
            uint64_t ival = 0;
            for (size_t j = 0; j < size; ++j)
                ival = (ival << 8) | static_cast<uint8_t>(data_[body_base + body_offset + j]);
            return std::to_string(ival);
        }
        if (serial_type >= 13 && serial_type % 2 == 1) {
            return std::string{
                reinterpret_cast<const char*>(data_.data() + body_base + body_offset), size};
        }
        return {};
    }

    auto parse_row_columns(size_t pos,
                           std::span<const size_t> column_indices,
                           uint64_t rowid,
                           const ColumnFilter* filter = nullptr) const
        -> std::optional<std::vector<std::string>> {
        auto header_start = pos;
        auto header_size_vr = read_varint(pos);
        pos += header_size_vr.consumed;
        auto header_end = header_start + header_size_vr.value;

        auto body_base = header_start + header_size_vr.value;
        size_t body_offset = 0;

        size_t max_col = column_indices.empty() ? 0 : *std::ranges::max_element(column_indices);
        if (filter)
            max_col = std::max(max_col, filter->column_index);

        std::vector<std::string> row(column_indices.size());
        std::string filter_val;

        for (size_t col = 0; pos < header_end && col <= max_col; ++col) {
            auto vr = read_varint(pos);
            pos += vr.consumed;
            auto sz = serial_type_size(vr.value);

            auto val = resolve_cell_value(vr.value, sz, rowid, body_base, body_offset);

            auto it = std::ranges::find(column_indices, col);
            if (it != column_indices.end()) {
                auto idx = static_cast<size_t>(it - column_indices.begin());
                row[idx] = val;
                if (filter && col == filter->column_index)
                    filter_val = val;
            } else if (filter && col == filter->column_index) {
                filter_val = std::move(val);
            }

            body_offset += sz;
        }

        if (filter && filter_val != filter->value)
            return std::nullopt;
        return row;
    }
  public:
    auto index_search(uint32_t page_number,
                      std::string_view target,
                      std::vector<uint64_t>& rowids) const -> void {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kIndexLeaf) {
            auto num_cells = read_u16_be(page_offset + 3);
            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);
                auto [val, rowid] = parse_index_cell(cell_ptr);
                if (val == target) {
                    rowids.push_back(rowid);
                } else if (val > target) {
                    break;
                }
            }
            return;
        }

        if (type == kIndexInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);

            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto left_child = read_u32_be(cell_ptr);
                auto [val, rowid] = parse_index_cell(cell_ptr + 4);

                if (val < target) {
                    continue;
                } else if (val == target) {
                    rowids.push_back(rowid);
                    index_search(left_child, target, rowids);
                } else {
                    index_search(left_child, target, rowids);
                    return;
                }
            }
            index_search(right_child, target, rowids);
            return;
        }
    }

    auto read_row_by_rowid(uint32_t page_number,
                           uint64_t target_rowid,
                           std::span<const size_t> column_indices) const
        -> std::optional<std::vector<std::string>> {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);

            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto child = read_u32_be(cell_ptr);
                auto key_vr = read_varint(cell_ptr + 4);

                if (target_rowid <= key_vr.value)
                    return read_row_by_rowid(child, target_rowid, column_indices);
            }
            return read_row_by_rowid(right_child, target_rowid, column_indices);
        }

        auto num_cells = read_u16_be(page_offset + 3);
        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);
            auto pos = cell_ptr;
            pos += read_varint(pos).consumed;
            auto rowid_vr = read_varint(pos);
            pos += rowid_vr.consumed;

            if (rowid_vr.value == target_rowid)
                return parse_row_columns(pos, column_indices, rowid_vr.value);
            if (rowid_vr.value > target_rowid)
                break;
        }
        return std::nullopt;
    }

    static auto open(std::string_view path) -> std::expected<Database, std::string> {
        std::ifstream file(std::string(path), std::ios::binary);
        if (!file)
            return std::unexpected("Failed to open database file");

        file.seekg(0, std::ios::end);
        auto size = file.tellg();
        file.seekg(0);

        std::vector<std::byte> buf(static_cast<size_t>(size));
        file.read(reinterpret_cast<char*>(buf.data()), size);
        if (!file)
            return std::unexpected("Failed to read database file");

        return Database(std::move(buf));
    }

    auto page_size() const noexcept -> uint32_t { return page_size_; }
    auto num_tables() const noexcept -> uint32_t { return num_tables_; }

    auto table_names() const -> std::vector<std::string_view> {
        std::vector<std::string_view> names;
        for (const auto& entry : schema_)
            if (!entry.name.starts_with("sqlite_"))
                names.push_back(entry.name);
        std::ranges::sort(names);
        return names;
    }

    auto rootpage(std::string_view table_name) const -> std::expected<uint32_t, std::string> {
        for (const auto& entry : schema_)
            if (util::iequals(entry.name, table_name))
                return entry.rootpage;
        return std::unexpected(std::format("Error: no such table: {}", table_name));
    }

    auto index_rootpage(std::string_view table_name, std::string_view column_name) const
        -> std::expected<uint32_t, std::string> {
        for (const auto& entry : schema_) {
            if (entry.type != "index")
                continue;
            if (entry.sql.find(table_name) == std::string_view::npos)
                continue;
            if (util::find_ci(entry.sql, column_name) != std::string_view::npos)
                return entry.rootpage;
        }
        return std::unexpected(std::format("No index for {}.{}", table_name, column_name));
    }

    auto count_rows(uint32_t page_number) const -> uint32_t {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableLeaf)
            return read_u16_be(page_offset + 3);

        auto num_cells = read_u16_be(page_offset + 3);
        auto right_child = read_u32_be(page_offset + 8);
        uint32_t total = count_rows(right_child);
        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
            auto child = read_u32_be(cell_ptr);
            total += count_rows(child);
        }
        return total;
    }

    auto table_sql(std::string_view table_name) const -> std::string_view {
        for (const auto& entry : schema_)
            if (util::iequals(entry.name, table_name))
                return entry.sql;
        return {};
    }

    auto parse_create_table(std::string_view sql) const -> std::vector<std::string_view> {
        auto paren_open = sql.find('(');
        auto paren_close = sql.rfind(')');
        auto body = sql.substr(paren_open + 1, paren_close - paren_open - 1);

        std::vector<std::string_view> columns;
        size_t pos = 0;
        while (pos < body.size()) {
            auto comma = body.find(',', pos);
            auto end = comma == std::string_view::npos ? body.size() : comma;
            auto trimmed = util::trim(body.substr(pos, end - pos));
            auto space = trimmed.find_first_of(" \t\n\r");
            if (space != std::string_view::npos)
                columns.push_back(trimmed.substr(0, space));
            pos = end + 1;
        }
        return columns;
    }

    auto read_columns_values(uint32_t page_number,
                             std::span<const size_t> column_indices,
                             const ColumnFilter* filter = nullptr) const
        -> std::vector<std::vector<std::string>> {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        auto type = static_cast<uint8_t>(data_[page_offset]);

        if (type == kTableInterior) {
            auto num_cells = read_u16_be(page_offset + 3);
            auto right_child = read_u32_be(page_offset + 8);

            std::vector<std::vector<std::string>> rows;
            for (uint32_t i = 0; i < num_cells; ++i) {
                auto cell_ptr = page_offset + read_u16_be(page_offset + 12 + i * 2);
                auto child = read_u32_be(cell_ptr);
                auto child_rows = read_columns_values(child, column_indices, filter);
                for (auto& r : child_rows)
                    rows.push_back(std::move(r));
            }
            auto right_rows = read_columns_values(right_child, column_indices, filter);
            for (auto& r : right_rows)
                rows.push_back(std::move(r));
            return rows;
        }

        auto num_cells = read_u16_be(page_offset + 3);
        std::vector<std::vector<std::string>> rows;
        rows.reserve(num_cells);

        for (uint32_t i = 0; i < num_cells; ++i) {
            auto cell_ptr = page_offset + read_u16_be(page_offset + 8 + i * 2);
            auto pos = cell_ptr;
            pos += read_varint(pos).consumed;
            auto rowid_vr = read_varint(pos);
            pos += rowid_vr.consumed;

            auto row = parse_row_columns(pos, column_indices, rowid_vr.value, filter);
            if (row)
                rows.push_back(std::move(*row));
        }
        return rows;
    }
};

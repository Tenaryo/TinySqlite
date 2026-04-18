#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <fstream>
#include <string>
#include <string_view>
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

    explicit Database(std::vector<std::byte> data)
        : data_(std::move(data)), page_size_(read_u16_be(16)), num_tables_(read_u16_be(103)) {
        for (auto offset : cell_offsets())
            schema_.push_back(parse_schema_entry(offset));
    }

    auto read_u16_be(size_t offset) const noexcept -> uint16_t {
        return static_cast<uint16_t>(data_[offset]) << 8 | static_cast<uint16_t>(data_[offset + 1]);
    }

    struct VarintResult {
        uint64_t value;
        size_t consumed;
    };

    auto read_varint(size_t offset) const noexcept -> VarintResult {
        uint64_t value = 0;
        for (int i = 0; i < 9; ++i) {
            auto byte = static_cast<uint8_t>(data_[offset + static_cast<size_t>(i)]);
            if (i == 8) {
                value = (value << 8) | byte;
                return {value, 9};
            }
            value = (value << 7) | static_cast<uint64_t>(byte & 0x7F);
            if ((byte & 0x80) == 0)
                return {value, static_cast<size_t>(i) + 1};
        }
        __builtin_unreachable();
    }

    static auto serial_type_size(uint64_t serial_type) noexcept -> size_t {
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
        static constexpr size_t kCellPtrArrayStart = 108;
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

        static constexpr int kNumFields = 5;
        uint64_t serial_types[kNumFields];
        for (int i = 0; i < kNumFields; ++i) {
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
  public:
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
            if (std::ranges::equal(entry.name, table_name, [](char a, char b) {
                    return std::tolower(a) == std::tolower(b);
                }))
                return entry.rootpage;
        return std::unexpected(std::format("Error: no such table: {}", table_name));
    }

    auto count_rows(uint32_t page_number) const -> uint32_t {
        auto page_offset = static_cast<size_t>(page_number - 1) * page_size_;
        return read_u16_be(page_offset + 3);
    }
};

auto handle_command(const Database& db, std::string_view command, std::ostream& out) -> void {
    auto istarts_with = [](std::string_view str, std::string_view prefix) {
        return str.size() >= prefix.size() &&
               std::ranges::equal(str.substr(0, prefix.size()), prefix, [](char a, char b) {
                   return std::tolower(a) == std::tolower(b);
               });
    };

    if (command == ".dbinfo") {
        out << "database page size: " << db.page_size() << '\n'
            << "number of tables: " << db.num_tables() << '\n';
    } else if (command == ".tables") {
        for (const auto& name : db.table_names())
            out << name << ' ';
        out << '\n';
    } else if (istarts_with(command, "SELECT COUNT")) {
        auto last_space = command.rfind(' ');
        auto table_name = command.substr(last_space + 1);
        auto rp = db.rootpage(table_name);
        if (!rp) {
            out << rp.error() << '\n';
            return;
        }
        out << db.count_rows(*rp) << '\n';
    }
}

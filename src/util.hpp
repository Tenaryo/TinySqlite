#pragma once

#include <algorithm>
#include <cctype>
#include <string_view>

namespace util {

inline auto to_lower(char c) -> char {
    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
}

inline auto find_ci(std::string_view haystack, std::string_view needle) -> size_t {
    if (haystack.size() < needle.size())
        return std::string_view::npos;
    for (size_t i = 0; i <= haystack.size() - needle.size(); ++i) {
        bool match = true;
        for (size_t j = 0; j < needle.size(); ++j) {
            if (to_lower(haystack[i + j]) != to_lower(needle[j])) {
                match = false;
                break;
            }
        }
        if (match)
            return i;
    }
    return std::string_view::npos;
}

inline auto iequals(std::string_view a, std::string_view b) -> bool {
    return std::ranges::equal(a, b, [](char x, char y) { return to_lower(x) == to_lower(y); });
}

inline auto trim(std::string_view s) -> std::string_view {
    while (!s.empty() &&
           (s.front() == ' ' || s.front() == '\t' || s.front() == '\n' || s.front() == '\r'))
        s.remove_prefix(1);
    while (!s.empty() &&
           (s.back() == ' ' || s.back() == '\t' || s.back() == '\n' || s.back() == '\r'))
        s.remove_suffix(1);
    return s;
}

} // namespace util

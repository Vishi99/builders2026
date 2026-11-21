#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__AVX2__) && defined(__BMI2__)
#include <immintrin.h>
#elif defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace input {

class MappedFile {
public:
    explicit MappedFile(const char* path) {
        const int fd = open(path, O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            throw std::system_error(errno, std::generic_category(), path);
        }
        struct stat status {};
        if (fstat(fd, &status) != 0) {
            const int error = errno;
            close(fd);
            throw std::system_error(error, std::generic_category(), path);
        }
        size_ = static_cast<std::size_t>(status.st_size);
        if (size_ != 0) {
            void* mapped = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
            if (mapped == MAP_FAILED) {
                const int error = errno;
                close(fd);
                throw std::system_error(error, std::generic_category(), path);
            }
            data_ = static_cast<const char*>(mapped);
        }
        close(fd);
    }

    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    MappedFile(MappedFile&& other) noexcept
        : data_(std::exchange(other.data_, nullptr)), size_(std::exchange(other.size_, 0)) {}

    MappedFile& operator=(MappedFile&& other) noexcept {
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
        return *this;
    }

    ~MappedFile() {
        if (data_ != nullptr) {
            munmap(const_cast<char*>(data_), size_);
        }
    }

    [[nodiscard]] const char* data() const noexcept { return data_; }
    [[nodiscard]] std::size_t size() const noexcept { return size_; }

private:
    const char* data_ = nullptr;
    std::size_t size_ = 0;
};

namespace detail {

inline const char* find_byte(const char* first, const char* last, char byte) noexcept {
    return static_cast<const char*>(std::memchr(first, byte, last - first));
}

inline const char* next_line(const char* first, const char* last) noexcept {
    const char* newline = find_byte(first, last, '\n');
    return newline == nullptr ? last : newline + 1;
}

inline bool decode_field(const char* first, const char* last, std::string_view& field) noexcept {
    if (first != last && *first == '"') {
        if (last - first < 2 || last[-1] != '"') {
            return false;
        }
        ++first;
        --last;
    }
    if (find_byte(first, last, '"') != nullptr) {
        return false;
    }
    field = std::string_view(first, last - first);
    return true;
}

template <std::size_t Width>
inline bool decode_row(const char* first, const char* last,
                       std::array<std::string_view, Width>& fields) noexcept {
    for (std::size_t column = 0; column + 1 < Width; ++column) {
        const char* comma = find_byte(first, last, ',');
        if (comma == nullptr || !decode_field(first, comma, fields[column])) {
            return false;
        }
        first = comma + 1;
    }
    return find_byte(first, last, ',') == nullptr && decode_field(first, last, fields.back());
}

#if (defined(__AVX2__) && defined(__BMI2__)) || (defined(__aarch64__) && defined(__ARM_NEON))
struct CsvMasks {
    std::uint64_t newlines;
    std::uint64_t commas;
    std::uint64_t quotes;
};

#if defined(__AVX512BW__)
inline constexpr std::size_t csv_block_bytes = 64;
inline constexpr unsigned csv_mask_stride = 1;

inline CsvMasks csv_masks(const char* block) noexcept {
    const auto bytes = _mm512_loadu_si512(block);
    return {_mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('\n')),
            _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8(',')),
            _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('"'))};
}
#elif defined(__AVX2__)
inline constexpr std::size_t csv_block_bytes = 32;
inline constexpr unsigned csv_mask_stride = 1;

inline CsvMasks csv_masks(const char* block) noexcept {
    const auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
    auto match = [&](char ch) {
        return static_cast<std::uint32_t>(_mm256_movemask_epi8(
            _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(ch))));
    };
    return {match('\n'), match(','), match('"')};
}
#else
inline constexpr std::size_t csv_block_bytes = 16;
inline constexpr unsigned csv_mask_stride = 4;

inline CsvMasks csv_masks(const char* block) noexcept {
    static_assert(std::endian::native == std::endian::little);
    const auto bytes = vld1q_u8(reinterpret_cast<const std::uint8_t*>(block));
    auto match = [&](char ch) {
        // Narrow each matching byte to a nibble and keep one bit per byte.
        const auto pairs = vreinterpretq_u16_u8(vceqq_u8(bytes, vdupq_n_u8(ch)));
        const auto packed = vshrn_n_u16(pairs, 4);
        return vget_lane_u64(vreinterpret_u64_u8(packed), 0) & 0x1111111111111111ULL;
    };
    return {match('\n'), match(','), match('"')};
}
#endif

#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
// Locate the LF and validate CSV structure from the same vector loads.
// Every comma separates fields, and wrapping quotes account for all quotes.
template <std::size_t Width>
inline bool read_vector_row(const char*& line, const char* last,
                            std::array<std::string_view, Width>& fields) noexcept {
    const char* row_start = line;
    const char* first = line;
    const char* block = line;
    std::size_t column = 0;
    std::size_t quote_count = 0;
    std::size_t wrapped_fields = 0;

    auto field = [&](const char* end) {
        const bool wrapped = first < end && *first == '"';
        if (wrapped && (end - first < 2 || end[-1] != '"')) {
            return false;
        }
        wrapped_fields += wrapped;
        fields[column++] = std::string_view(first + wrapped, end - first - 2 * wrapped);
        first = end + 1;
        return true;
    };

    for (; last - block >= static_cast<std::ptrdiff_t>(csv_block_bytes); block += csv_block_bytes) {
        const auto masks = csv_masks(block);
        const auto newline = masks.newlines & (0 - masks.newlines);
        // An absent LF produces an all-ones mask after unsigned subtraction.
        const auto before_lf = newline - 1;
        quote_count += std::popcount(masks.quotes & before_lf);
        auto commas = masks.commas & before_lf;
        while (commas != 0) {
            const auto* comma = block + std::countr_zero(commas) / csv_mask_stride;
            if (column + 1 >= Width || !field(comma)) {
                line = newline == 0 ? next_line(block + csv_block_bytes, last)
                    : block + std::countr_zero(newline) / csv_mask_stride + 1;
                return false;
            }
            commas &= commas - 1;
        }
        if (newline != 0) {
            const auto* end = block + std::countr_zero(newline) / csv_mask_stride;
            line = end + 1;
            return end != row_start && column + 1 == Width && field(end) &&
                   quote_count == 2 * wrapped_fields;
        }
    }

    // At the map tail, avoid reading a vector across the end of the mapping.
    const auto* newline = find_byte(block, last, '\n');
    const auto* end = newline == nullptr ? last : newline;
    line = newline == nullptr ? last : newline + 1;
    return end != row_start && decode_row(row_start, end, fields);
}
#endif

#if defined(__AVX2__) && defined(__BMI2__)
// Use this decoder only when the map still has enough bytes for the last load.
// Count every quote, then prove each is one of a field's two wrapping quotes.
template <std::size_t Width>
inline bool decode_padded_row(const char* first, const char* last,
                              std::array<std::string_view, Width>& fields) noexcept {
    const auto comma_bytes = _mm256_set1_epi8(',');
    const auto quote_bytes = _mm256_set1_epi8('"');
    std::size_t column = 0;
    std::size_t quote_count = 0;
    std::size_t wrapped_fields = 0;
    auto field = [&](const char* end) {
        const auto length = end - first;
        // Rows follow a header, so even a leading empty field has a preceding byte.
        const bool wrapped = (length >= 2) & (*first == '"') & (end[-1] == '"');
        wrapped_fields += wrapped;
        fields[column++] = std::string_view(first + wrapped, length - 2 * wrapped);
        first = end + 1;
    };
    for (const char* block = first; block < last;) {
        const auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block));
        const unsigned size = std::min<std::ptrdiff_t>(last - block, 32);
        const auto quotes = _bzhi_u32(
            static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, quote_bytes))), size);
        auto commas = _bzhi_u32(
            static_cast<unsigned>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, comma_bytes))), size);
        quote_count += std::popcount(quotes);
        while (commas != 0) {
            if (column + 1 >= Width) {
                return false;
            }
            field(block + std::countr_zero(commas));
            commas &= commas - 1;
        }
        block += size;
    }
    if (column + 1 != Width) {
        return false;
    }
    field(last);
    return quote_count == 2 * wrapped_fields;
}

struct ByteMasks {
    std::uint32_t newline, comma, quote;
};

inline ByteMasks classify(const char* position) noexcept {
    const auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(position));
    auto mask = [&](char byte) {
        return static_cast<std::uint32_t>(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(byte))));
    };
    return {mask('\n'), mask(','), mask('"')};
}

template <std::size_t Width>
inline constexpr std::ptrdiff_t short_scan_bytes = Width == 7 ? 96 : 64;

template <std::size_t Width>
inline bool decode_masked_fields(const char* line, const char* last, std::uint64_t commas,
                                 std::uint64_t late_commas, unsigned quote_count,
                                 std::array<std::string_view, Width>& fields) noexcept {
    if (std::popcount(commas) + std::popcount(late_commas) != Width - 1) {
        return false;
    }
    unsigned wrapped_fields = 0;
    const char* start = line;
    auto field = [&](std::size_t column, const char* end) {
        const auto size = end - start;
        const bool wrapped = (size >= 2) & (*start == '"') & (end[-1] == '"');
        wrapped_fields += wrapped;
        fields[column] = std::string_view(start + wrapped, size - 2 * wrapped);
        start = end + 1;
    };
    for (std::size_t column = 0; column < Width - 1; ++column) {
        unsigned boundary;
        if (commas != 0) {
            boundary = std::countr_zero(commas);
            commas &= commas - 1;
        } else {
            boundary = 64 + std::countr_zero(late_commas);
            late_commas &= late_commas - 1;
        }
        field(column, line + boundary);
    }
    field(Width - 1, last);
    return quote_count == 2 * wrapped_fields;
}

// The caller guarantees that the vector loads stay within the map. A missing
// LF delegates the whole row to the generic decoder.
template <std::size_t Width>
inline const char* decode_short_row(const char* line, std::array<std::string_view, Width>& fields,
                                    bool& valid) noexcept {
    static_assert(Width == 4 || Width == 7);
    const auto first = classify(line);
    const auto second = classify(line + 32);
    const auto newlines = (std::uint64_t{second.newline} << 32) | first.newline;
    ByteMasks third{};
    unsigned length = std::countr_zero(newlines);
    if constexpr (Width == 7) {
        third = classify(line + 64);
        length = newlines != 0 ? length : 64 + std::countr_zero(third.newline);
    }
    if (length == short_scan_bytes<Width>) {
        return nullptr;
    }

    valid = false;
    const char* const last = line + length;
    const unsigned tail = length > 64 ? length - 64 : 0;
    auto commas = _bzhi_u64((std::uint64_t{second.comma} << 32) | first.comma, length);
    auto late_commas = _bzhi_u32(third.comma, tail);
    const auto quotes = _bzhi_u64((std::uint64_t{second.quote} << 32) | first.quote, length);
    const auto late_quotes = _bzhi_u32(third.quote, tail);
    const unsigned quote_count = std::popcount(quotes) + std::popcount(late_quotes);
    valid = decode_masked_fields(line, last, commas, late_commas, quote_count, fields);
    return last;
}

struct BlockMasks {
    std::uint64_t newline, comma, quote;
};

inline BlockMasks classify_block(const char* base) noexcept {
    const auto low = classify(base);
    const auto high = classify(base + 32);
    return {(std::uint64_t{high.newline} << 32) | low.newline,
            (std::uint64_t{high.comma} << 32) | low.comma,
            (std::uint64_t{high.quote} << 32) | low.quote};
}

inline std::pair<std::uint64_t, std::uint64_t> row_bits(
    std::uint64_t first, std::uint64_t next, std::uint64_t third,
    unsigned skip, unsigned length) noexcept {
    if (skip != 0) {
        const unsigned carry = 64 - skip;
        first = (first >> skip) | (next << carry);
        next = (next >> skip) | (third << carry);
    }
    return {_bzhi_u64(first, length), _bzhi_u64(next, length > 64 ? length - 64 : 0)};
}

// The row start belongs to this byte range. Loads may cross range boundaries
// while the backing file contains the complete lookahead block.
template <typename Callback>
const char* scan_rolling_events(const char* line, const char* limit, const char* last,
                               std::array<std::string_view, 7>& fields, Callback& callback) {
    constexpr unsigned block_bytes = 64;
    constexpr unsigned lookahead_bytes = 3 * block_bytes;
    if (line >= limit || last - line < lookahead_bytes) {
        return line;
    }
    const char* base = line;
    auto first = classify_block(base);
    auto second = classify_block(base + block_bytes);
    auto third = classify_block(base + 2 * block_bytes);
    while (true) {
        const unsigned skip = static_cast<unsigned>(line - base);
        const auto first_lfs = first.newline & (~std::uint64_t{0} << skip);
        const unsigned end_offset = first_lfs != 0 ? std::countr_zero(first_lfs)
            : second.newline != 0 ? block_bytes + std::countr_zero(second.newline)
            : 2 * block_bytes + std::countr_zero(third.newline);
        const unsigned length = end_offset - skip;
        if (length > 2 * block_bytes) {
            return line;
        }

        const char* const end = base + end_offset;
        const auto [commas, late_commas] = row_bits(first.comma, second.comma, third.comma, skip, length);
        const auto [quotes, late_quotes] = row_bits(first.quote, second.quote, third.quote, skip, length);
        if (decode_masked_fields(line, end, commas, late_commas,
                                 std::popcount(quotes) + std::popcount(late_quotes), fields)) {
            callback(std::as_const(fields));
        }

        line = end + 1;
        if (line >= limit) {
            return line;
        }
        const unsigned advance = (end_offset + 1) / block_bytes;
        if (advance == 0) {
            continue;
        }
        base += advance * block_bytes;
        if (last - base < lookahead_bytes) {
            return line;
        }
        if (advance == 1) {
            first = second;
            second = third;
        } else {
            first = advance == 2 ? third : classify_block(base);
            second = classify_block(base + block_bytes);
        }
        third = classify_block(base + 2 * block_bytes);
    }
}

#if defined(__AVX512BW__)
inline BlockMasks classify_event_block64(const char* line) noexcept {
    const auto bytes = _mm512_loadu_si512(line);
    return {_mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('\n')),
            _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8(',')),
            _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('"'))};
}

// Row ownership follows the first byte. The load guard uses the mapping end
// because a range may own a row that continues after its scheduling boundary.
template <typename Callback>
const char* scan_avx512_events(const char* line, const char* limit, const char* last,
                              std::array<std::string_view, 7>& fields, Callback& callback) {
    constexpr unsigned block_bytes = 64;
    while (line < limit && last - line >= block_bytes) {
        const auto first = classify_event_block64(line);
        BlockMasks second{};
        unsigned length = std::countr_zero(first.newline);
        if (first.newline == 0) {
            if (last - line < 2 * block_bytes) {
                break;
            }
            second = classify_event_block64(line + block_bytes);
            length = block_bytes + std::countr_zero(second.newline);
        }
        if (length == 2 * block_bytes) {
            // The complete vector prefix had no LF. Finish this row with
            // the general decoder, then resume the short-row path.
            const char* newline = find_byte(line + length, last, '\n');
            const char* end = newline == nullptr ? last : newline;
            const bool valid = last - end >= 31 ? decode_padded_row(line, end, fields)
                                                  : decode_row(line, end, fields);
            if (valid) {
                callback(std::as_const(fields));
            }
            line = newline == nullptr ? last : newline + 1;
            continue;
        }

        const char* const end = line + length;
        const unsigned tail = length > block_bytes ? length - block_bytes : 0;
        const auto commas = _bzhi_u64(first.comma, length);
        const auto late_commas = _bzhi_u64(second.comma, tail);
        const auto quotes = _bzhi_u64(first.quote, length);
        const auto late_quotes = _bzhi_u64(second.quote, tail);
        if (decode_masked_fields(line, end, commas, late_commas,
                                 std::popcount(quotes) + std::popcount(late_quotes), fields)) {
            callback(std::as_const(fields));
        }
        line = end + 1;
    }
    return line;
}
#endif

#endif

template <typename Integer>
inline Integer unsigned_decimal(std::string_view value, Integer invalid) noexcept {
    if (value.empty()) {
        return invalid;
    }
    Integer result;
    const char* const end = value.data() + value.size();
    const auto parsed = std::from_chars(value.data(), end, result);
    return parsed.ec == std::errc{} && parsed.ptr == end ? result : invalid;
}

#if defined(__AVX2__) && defined(__BMI2__)
// Two bounded loads cover exactly 13 bytes. Pad the last five digits with
// three ASCII zeroes so both eight-byte lanes can share the decimal reduction.
inline std::int64_t milliseconds_13(const char* text, std::int64_t invalid) noexcept {
    const auto raw = _mm_unpacklo_epi64(_mm_loadu_si64(text), _mm_loadu_si64(text + 5));
    const auto padded = _mm_or_si128(
        _mm_and_si128(raw, _mm_set_epi64x(-0x1000000LL, -1LL)),
        _mm_set_epi64x(0x303030, 0));
    const auto digits = _mm_sub_epi8(padded, _mm_set1_epi8('0'));
    const auto outside = _mm_subs_epu8(digits, _mm_set1_epi8(9));
    if (!_mm_testz_si128(outside, outside)) {
        return invalid;
    }
    const auto pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
    const auto groups = _mm_madd_epi16(pairs, _mm_set1_epi32(0x00010064));
    const auto words = _mm_packus_epi32(groups, groups);
    const auto values = _mm_madd_epi16(words, _mm_set1_epi32(0x00012710));
    return std::int64_t{_mm_cvtsi128_si32(values)} * 100'000 + _mm_extract_epi32(values, 1);
}
#endif

inline int decimal_pair(const char* value) noexcept {
    const unsigned first = static_cast<unsigned char>(value[0]) - '0';
    const unsigned second = static_cast<unsigned char>(value[1]) - '0';
    return first <= 9 && second <= 9 ? static_cast<int>(first * 10 + second) : -1;
}

struct CalendarMonth {
    int first_day;
    int length;
};

constexpr auto make_calendar_months() {
    using namespace std::chrono;
    std::array<std::array<CalendarMonth, 12>, 400> months{};
    for (int y = 0; y < 400; ++y) {
        for (unsigned m = 1; m <= 12; ++m) {
            const auto month_start = year(y) / month(m);
            months[y][m - 1] = {
                static_cast<int>(sys_days(month_start / 1).time_since_epoch().count()),
                static_cast<int>(static_cast<unsigned>((month_start / last).day()))
            };
        }
    }
    return months;
}

// A Gregorian cycle has 400 years and exactly 146,097 days.
inline constexpr auto calendar_months = make_calendar_months();

}  // namespace detail

// Byte ranges can be scanned concurrently. A range owns rows whose first byte
// it contains, even if the row ends in the next range. Views borrow the input.
template <std::size_t Width, typename Callback>
void for_each_row_bytes(const char* data, std::size_t size, std::size_t begin,
                        std::size_t end, Callback&& callback) {
    static_assert(Width > 0);
    end = std::min(end, size);
    if (begin >= end) {
        return;
    }

    const char* const last = data + size;
    const char* const limit = data + end;
    const char* line = data + begin;
    if (begin == 0 || line[-1] != '\n') {
        line = detail::next_line(line, last);
    }

    std::array<std::string_view, Width> fields;
#if defined(__AVX2__) && defined(__BMI2__)
    if constexpr (Width == 7) {
#if defined(__AVX512BW__)
        line = detail::scan_avx512_events(line, limit, last, fields, callback);
#else
        line = detail::scan_rolling_events(line, limit, last, fields, callback);
#endif
    }
#endif
    while (line < limit) {
#if defined(__AVX2__) && defined(__BMI2__)
        if constexpr (Width == 4 || Width == 7) {
            if (last - line >= detail::short_scan_bytes<Width>) {
                bool valid;
                if (const char* newline = detail::decode_short_row(line, fields, valid)) {
                    if (valid) {
                        callback(std::as_const(fields));
                    }
                    line = newline + 1;
                    continue;
                }
            }
        }
#endif
#if defined(__aarch64__) && defined(__ARM_NEON)
        const bool valid = detail::read_vector_row(line, last, fields);
#else
        const char* newline = detail::find_byte(line, last, '\n');
        const char* const line_end = newline == nullptr ? last : newline;
#if defined(__AVX2__) && defined(__BMI2__)
        const bool valid = line != line_end && (last - line_end >= 31
            ? detail::decode_padded_row(line, line_end, fields)
            : detail::decode_row(line, line_end, fields));
#else
        const bool valid = line != line_end && detail::decode_row(line, line_end, fields);
#endif
        line = newline == nullptr ? last : newline + 1;
#endif
        if (valid) {
            callback(std::as_const(fields));
        }
    }
}

template <std::size_t Width, typename Callback>
void for_each_row(const MappedFile& file, Callback&& callback) {
    for_each_row_bytes<Width>(file.data(), file.size(), 0, file.size(),
                             std::forward<Callback>(callback));
}

class BufferedFile {
public:
    explicit BufferedFile(const char* path) : fd_(open(path, O_RDONLY | O_CLOEXEC)) {
        if (fd_ < 0) {
            throw std::system_error(errno, std::generic_category(), path);
        }
        struct stat info {};
        if (fstat(fd_, &info) != 0) {
            const auto error = errno;
            close(fd_);
            throw std::system_error(error, std::generic_category(), path);
        }
        size_ = static_cast<std::size_t>(info.st_size);
    }

    BufferedFile(BufferedFile&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)), size_(other.size_) {}

    ~BufferedFile() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    [[nodiscard]] std::size_t size() const noexcept { return size_; }

    // Workers share the descriptor. Explicit offsets let them read independently.
    void read(char* destination, std::size_t offset, std::size_t count) const {
        while (count != 0) {
            const auto bytes = pread(fd_, destination, count, static_cast<off_t>(offset));
            if (bytes < 0 && errno == EINTR) {
                continue;
            }
            if (bytes < 0) {
                throw std::system_error(errno, std::generic_category(), "pread CSV");
            }
            if (bytes == 0) {
                throw std::runtime_error("Input shortened during CSV scan");
            }
            destination += bytes;
            offset += bytes;
            count -= bytes;
        }
    }

private:
    int fd_;
    std::size_t size_;
};

class ChunkBuffer {
public:
    void reserve(std::size_t bytes, std::size_t retain = 0) {
        if (bytes <= capacity_) {
            return;
        }
        auto next = std::make_unique_for_overwrite<char[]>(bytes);
        if (retain != 0) {
            std::memcpy(next.get(), storage_.get(), retain);
        }
        storage_ = std::move(next);
        capacity_ = bytes;
    }

    [[nodiscard]] char* data() const noexcept { return storage_.get(); }

private:
    std::unique_ptr<char[]> storage_;
    std::size_t capacity_ = 0;
};

template <std::size_t Width, typename Callback>
void for_each_row(const BufferedFile& file, ChunkBuffer& buffer, std::size_t begin,
                  std::size_t end, Callback&& callback) {
    // Read one preceding byte to distinguish a row start from a partial row.
    const std::size_t first = begin != 0;
    const std::size_t offset = begin - first;
    const std::size_t owned_end = end - offset;
    std::size_t loaded = std::min(file.size() - offset, owned_end + 4096);
    buffer.reserve(loaded);
    file.read(buffer.data(), offset, loaded);

    if ((first == 0 || buffer.data()[0] != '\n') &&
        std::memchr(buffer.data() + first, '\n', owned_end - first) == nullptr) {
        // The chunk contains only a header or part of a row owned by another chunk.
        return;
    }

    if (buffer.data()[owned_end - 1] != '\n' &&
        std::memchr(buffer.data() + owned_end, '\n', loaded - owned_end) == nullptr) {
        // A row that starts in this chunk belongs here through its trailing LF or EOF.
        while (offset + loaded < file.size()) {
            const auto extra = std::min(file.size() - offset - loaded, loaded);
            buffer.reserve(loaded + extra, loaded);
            file.read(buffer.data() + loaded, offset + loaded, extra);
            const bool end_of_row = std::memchr(buffer.data() + loaded, '\n', extra) != nullptr;
            loaded += extra;
            if (end_of_row) {
                break;
            }
        }
    }
    for_each_row_bytes<Width>(buffer.data(), loaded, first, owned_end,
                             std::forward<Callback>(callback));
}

inline constexpr std::int64_t invalid_timestamp = std::numeric_limits<std::int64_t>::min();

inline std::int64_t timestamp_ms(std::string_view value) noexcept {
    const bool utc = value.size() == 20 && value[19] == 'Z';
    const bool offset = value.size() == 25 && (value[19] == '+' || value[19] == '-');
    if (!utc && !offset) {
#if defined(__AVX2__) && defined(__BMI2__)
        if (value.size() == 13) {
            return detail::milliseconds_13(value.data(), invalid_timestamp);
        }
#endif
        // Parsing an unsigned integer rejects a leading sign and accepts zero padding.
        constexpr auto maximum = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
        const auto parsed = detail::unsigned_decimal(value, maximum + 1);
        return parsed <= maximum ? static_cast<std::int64_t>(parsed) : invalid_timestamp;
    }

    const char* const text = value.data();
#if defined(__AVX2__) && defined(__BMI2__)
    // Validate the ASCII layout and convert the first six pairs of digits together.
    const auto prefix = _mm_loadu_si128(reinterpret_cast<const __m128i*>(text));
    const auto lower = _mm_loadu_si128(reinterpret_cast<const __m128i*>("0000-00-00T00:00"));
    const auto upper = _mm_loadu_si128(reinterpret_cast<const __m128i*>("9999-99-99T99:99"));
    const auto bad = _mm_or_si128(_mm_cmplt_epi8(prefix, lower), _mm_cmpgt_epi8(prefix, upper));
    if (_mm_movemask_epi8(bad) != 0 || text[16] != ':') {
        return invalid_timestamp;
    }
    const auto positions = _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1);
    const auto digits = _mm_sub_epi8(_mm_shuffle_epi8(prefix, positions), _mm_set1_epi8('0'));
    const auto pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
    const int century = _mm_extract_epi16(pairs, 0);
    const int year_in_century = _mm_extract_epi16(pairs, 1);
    const int month = _mm_extract_epi16(pairs, 2);
    const int day = _mm_extract_epi16(pairs, 3);
    const int hour = _mm_extract_epi16(pairs, 4);
    const int minute = _mm_extract_epi16(pairs, 5);
#else
    if (value[4] != '-' || value[7] != '-' || value[10] != 'T' ||
        value[13] != ':' || value[16] != ':') {
        return invalid_timestamp;
    }
    const int century = detail::decimal_pair(text);
    const int year_in_century = detail::decimal_pair(text + 2);
    const int month = detail::decimal_pair(text + 5);
    const int day = detail::decimal_pair(text + 8);
    const int hour = detail::decimal_pair(text + 11);
    const int minute = detail::decimal_pair(text + 14);
#endif
    const int second = detail::decimal_pair(text + 17);
    if (century < 0 || year_in_century < 0 || month < 1 || month > 12 || day < 1 ||
        static_cast<unsigned>(hour) > 23 || static_cast<unsigned>(minute) > 59 ||
        static_cast<unsigned>(second) > 59) {
        return invalid_timestamp;
    }
    const auto& calendar = detail::calendar_months[(century % 4) * 100 + year_in_century][month - 1];
    if (day > calendar.length) {
        return invalid_timestamp;
    }
    int offset_minutes = 0;
    if (offset) {
        const int offset_hour = detail::decimal_pair(text + 20);
        const int offset_minute = detail::decimal_pair(text + 23);
        if (value[22] != ':' || static_cast<unsigned>(offset_hour) > 23 ||
            static_cast<unsigned>(offset_minute) > 59) {
            return invalid_timestamp;
        }
        const int sign = value[19] == '+' ? 1 : -1;
        offset_minutes = sign * (offset_hour * 60 + offset_minute);
    }
    const auto days = calendar.first_day + (century / 4) * 146'097 + day - 1;
    return static_cast<std::int64_t>(days) * 86'400'000 +
           ((hour * 60 + minute - offset_minutes) * 60 + second) * 1000;
}

inline std::int64_t ingest_ms(std::string_view value) noexcept {
    return timestamp_ms(value);
}

inline std::uint32_t purchase_price(std::string_view value) noexcept {
    const auto price = detail::unsigned_decimal(value, std::uint32_t{0});
    return price <= 10'000'000 ? price : 0;
}

}  // namespace input

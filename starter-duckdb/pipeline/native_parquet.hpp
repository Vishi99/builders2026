#pragma once

#include "native_output.hpp"

#include <array>
#include <bit>
#include <fstream>
#include <memory>
#include <span>

#include <lz4.h>

namespace shopping::parquet_output {

// Parquet's flat schema and V2 pages use Apache Thrift's compact protocol.
enum class Wire : uint8_t {
    True = 1, False = 2, I32 = 5, I64 = 6, Binary = 8, List = 9, Struct = 12
};

inline void varint(std::vector<char>& out, uint64_t value) {
    while (value >= 128) {
        out.push_back(static_cast<char>((value & 127) | 128));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

class Compact {
public:
    explicit Compact(std::vector<char>& out) : out_(out) {}

    void i32(int id, int32_t value) {
        field(id, Wire::I32);
        integer(value);
    }

    void i64(int id, int64_t value) {
        field(id, Wire::I64);
        integer(value);
    }

    void boolean(int id, bool value) {
        field(id, value ? Wire::True : Wire::False);
    }

    void binary(int id, std::string_view value) {
        field(id, Wire::Binary);
        binary(value);
    }

    void binary(std::string_view value) {
        varint(out_, value.size());
        out_.insert(out_.end(), value.begin(), value.end());
    }

    void integer(int64_t value) {
        varint(out_, (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(-(value < 0)));
    }

    void list(int id, Wire wire, size_t count) {
        field(id, Wire::List);
        out_.push_back(static_cast<char>((std::min<size_t>(count, 15) << 4) | static_cast<uint8_t>(wire)));
        if (count >= 15) {
            varint(out_, count);
        }
    }

    template<class Write>
    void structure(Write&& write) {
        Compact nested(out_);
        write(nested);
        nested.end();
    }

    template<class Write>
    void structure(int id, Write&& write) {
        field(id, Wire::Struct);
        structure(std::forward<Write>(write));
    }

    void end() { out_.push_back(0); }

private:
    void field(int id, Wire wire) {
        const auto delta = id - last_;
        const auto number = static_cast<uint8_t>(wire);
        if (delta > 0 && delta <= 15) {
            out_.push_back(static_cast<char>((delta << 4) | number));
        } else {
            out_.push_back(static_cast<char>(number));
            integer(id);
        }
        last_ = id;
    }

    std::vector<char>& out_;
    int last_ = 0;
};

enum Physical : int32_t { Int32 = 1, Int64 = 2, ByteArray = 6 };
enum Converted : int32_t { Unannotated = -1, Utf8 = 0, Date = 6, UInt32 = 13 };
inline constexpr int32_t plain = 0;
inline constexpr int32_t rle = 3;
inline constexpr int32_t data_page_v2 = 3;
inline constexpr int32_t lz4_raw = 7;
inline constexpr size_t row_group_rows = 122'880;

struct Field {
    std::string_view name;
    Physical physical;
    Converted converted = Unannotated;
    bool nullable = false;
};

inline constexpr std::array user_schema{
    Field{"user_id", ByteArray, Utf8}, Field{"day", Int32, Date}, Field{"sessions", Int32, UInt32},
    Field{"events", Int32, UInt32}, Field{"revenue_cents", Int64},
    Field{"top_category", ByteArray, Utf8, true}
};
inline constexpr std::array campaign_schema{
    Field{"campaign_id", ByteArray, Utf8}, Field{"channel", ByteArray, Utf8, true},
    Field{"day", Int32, Date}, Field{"sessions", Int64}, Field{"purchasing_sessions", Int64},
    Field{"revenue_cents", Int64}, Field{"assisted_sessions", Int64}
};

struct Chunk {
    int64_t offset;
    int64_t compressed;
    int64_t uncompressed;
};

struct RowGroup {
    std::array<Chunk, 7> columns;
    int64_t rows;
    int64_t uncompressed = 0;
};

struct Scratch {
    std::vector<char> levels, header;
    std::unique_ptr<char[]> strings, compressed;
    size_t string_capacity = 0;
    size_t compressed_capacity = 0;

    char* buffer(size_t limit, std::unique_ptr<char[]>& scratch, size_t& capacity) {
        if (limit > capacity) {
            scratch = std::make_unique_for_overwrite<char[]>(limit);
            capacity = limit;
        }
        return scratch.get();
    }
};

inline void append(std::ofstream& file, std::span<const char> bytes) {
    if (!bytes.empty()) {
        file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
}

inline uint32_t definition_levels(const Column& column, bool nullable, size_t first, size_t count,
                                  std::vector<char>& levels) {
    levels.clear();
    if (!nullable) {
        return 0;
    }
    if (column.validity.address == nullptr) {
        varint(levels, count << 1);
        levels.push_back(1);
        return 0;
    }

    // One bit-packed hybrid run can use the Arrow validity bits directly.
    const size_t bytes = (count + 7) / 8;
    const auto* bitmap = static_cast<const uint8_t*>(column.validity.address);
    const auto shift = first % 8;
    varint(levels, (bytes << 1) | 1);
    const size_t packed = levels.size();
    if (shift == 0) {
        levels.insert(levels.end(), bitmap + first / 8, bitmap + first / 8 + bytes);
    } else {
        for (size_t index = first / 8; index < first / 8 + bytes; ++index) {
            unsigned code = bitmap[index] >> shift;
            if (index + 1 < static_cast<size_t>(column.validity.size)) {
                code |= static_cast<unsigned>(bitmap[index + 1]) << (8 - shift);
            }
            levels.push_back(static_cast<char>(code));
        }
    }
    if (count % 8 != 0) {
        levels.back() = static_cast<char>(static_cast<uint8_t>(levels.back()) & ((1 << (count % 8)) - 1));
    }
    size_t valid = 0;
    for (size_t index = packed; index < levels.size(); ++index) {
        valid += std::popcount(static_cast<uint8_t>(levels[index]));
    }
    return static_cast<uint32_t>(count - valid);
}

inline std::span<const char> plain_values(const Column& column, Field field, size_t first, size_t count,
                                          Scratch& scratch) {
    const auto* source = static_cast<const char*>(column.values.address);
    if (field.physical != ByteArray) {
        const size_t width = field.physical == Int32 ? 4 : 8;
        return {source + first * width, count * width};
    }

    const auto* offsets = static_cast<const int64_t*>(column.offsets.address);
    const auto* validity = static_cast<const uint8_t*>(column.validity.address);
    const auto end = first + count;
    const auto max_bytes = static_cast<size_t>(offsets[end] - offsets[first]) + count * sizeof(uint32_t);
    char* out = scratch.buffer(max_bytes, scratch.strings, scratch.string_capacity);
    char* const begin = out;

    for (size_t i = first; i < end; ++i) {
        if (validity != nullptr && !(validity[i / 8] & (1 << (i % 8)))) {
            continue;
        }
        const auto length = static_cast<uint32_t>(offsets[i + 1] - offsets[i]);
        std::memcpy(out, &length, sizeof(length));
        out += sizeof(length);
        if (length != 0) {
            std::memcpy(out, source + offsets[i], length);
            out += length;
        }
    }
    return {begin, static_cast<size_t>(out - begin)};
}

inline Chunk write_chunk(std::ofstream& file, const Column& column, Field field,
                         size_t first, size_t count, Scratch& scratch) {
    const auto nulls = definition_levels(column, field.nullable, first, count, scratch.levels);
    const auto values = plain_values(column, field, first, count, scratch);
    std::span<const char> payload = values;
    if (!values.empty() && values.size() <= LZ4_MAX_INPUT_SIZE) {
        const auto capacity = LZ4_compressBound(static_cast<int>(values.size()));
        char* const compressed = scratch.buffer(capacity, scratch.compressed, scratch.compressed_capacity);
        const auto size = LZ4_compress_default(values.data(), compressed, static_cast<int>(values.size()),
                                               capacity);
        if (size > 0 && static_cast<size_t>(size) < values.size()) {
            payload = {compressed, static_cast<size_t>(size)};
        }
    }

    const auto bytes_uncompressed = scratch.levels.size() + values.size();
    const auto bytes_compressed = scratch.levels.size() + payload.size();
    if (bytes_uncompressed > INT32_MAX || bytes_compressed > INT32_MAX) {
        throw std::length_error("Parquet page exceeds its size limit");
    }
    scratch.header.clear();
    Compact header(scratch.header);
    header.i32(1, data_page_v2);
    header.i32(2, bytes_uncompressed);
    header.i32(3, bytes_compressed);
    header.structure(8, [&](Compact& page) {
        page.i32(1, count);
        page.i32(2, nulls);
        page.i32(3, count);
        page.i32(4, plain);
        page.i32(5, scratch.levels.size());
        page.i32(6, 0);
        page.boolean(7, payload.size() != values.size());
    });
    header.end();

    const auto offset = static_cast<int64_t>(file.tellp());
    append(file, scratch.header);
    append(file, scratch.levels);
    append(file, payload);
    return {offset, static_cast<int64_t>(bytes_compressed + scratch.header.size()),
                    static_cast<int64_t>(bytes_uncompressed + scratch.header.size())};
}

inline void write_footer(std::ofstream& file, std::span<const Field> schema,
                         size_t rows, const std::vector<RowGroup>& groups) {
    std::vector<char> bytes;
    Compact footer(bytes);
    footer.i32(1, 1);
    footer.list(2, Wire::Struct, schema.size() + 1);
    footer.structure([&](Compact& root) {
        root.binary(4, "schema");
        root.i32(5, schema.size());
    });
    for (const auto field : schema) {
        footer.structure([&](Compact& leaf) {
            leaf.i32(1, field.physical);
            leaf.i32(3, field.nullable ? 1 : 0);
            leaf.binary(4, field.name);
            if (field.converted != Unannotated) {
                leaf.i32(6, field.converted);
            }
        });
    }

    footer.i64(3, rows);
    footer.list(4, Wire::Struct, groups.size());
    for (const auto& group : groups) {
        footer.structure([&](Compact& row_group) {
            row_group.list(1, Wire::Struct, schema.size());
            for (size_t index = 0; index < schema.size(); ++index) {
                const auto field = schema[index];
                const auto& chunk = group.columns[index];
                row_group.structure([&](Compact& column) {
                    column.i64(2, 0);
                    column.structure(3, [&](Compact& meta) {
                        meta.i32(1, field.physical);
                        meta.list(2, Wire::I32, field.nullable ? 2 : 1);
                        meta.integer(plain);
                        if (field.nullable) {
                            meta.integer(rle);
                        }
                        meta.list(3, Wire::Binary, 1);
                        meta.binary(field.name);
                        meta.i32(4, lz4_raw);
                        meta.i64(5, group.rows);
                        meta.i64(6, chunk.uncompressed);
                        meta.i64(7, chunk.compressed);
                        meta.i64(9, chunk.offset);
                    });
                });
            }
            row_group.i64(2, group.uncompressed);
            row_group.i64(3, group.rows);
        });
    }
    footer.binary(6, "shopping-sessions version 1");
    footer.end();

    if (bytes.size() > UINT32_MAX) {
        throw std::length_error("Parquet footer exceeds its size limit");
    }
    const auto length = static_cast<uint32_t>(bytes.size());
    append(file, bytes);
    file.write(reinterpret_cast<const char*>(&length), sizeof(length));
    file.write("PAR1", 4);
}

inline size_t bounded_group(const Table& table, std::span<const Field> schema,
                            size_t first, size_t length) {
    // Keep page sizes representable even when input IDs are unusually long.
    constexpr int64_t max_text = 1 << 28;
    for (size_t index = 0; index < schema.size(); ++index) {
        if (schema[index].physical == ByteArray) {
            const auto* offsets = static_cast<const int64_t*>(table.columns[index].offsets.address);
            while (offsets[first + length] - offsets[first] > max_text) {
                if (length == 1) {
                    if (offsets[first + 1] - offsets[first] > INT32_MAX - 32) {
                        throw std::length_error("String exceeds the Parquet page size limit");
                    }
                    break;
                }
                length = length >= 16 ? (length / 16) * 8 : length / 2;
            }
        }
    }
    return length;
}

inline void write_file(const Table& table, std::span<const Field> schema,
                       size_t first, size_t last, const fs::path& path) {
    static_assert(std::endian::native == std::endian::little);
    std::ofstream file;
    file.exceptions(std::ios::failbit | std::ios::badbit);
    file.open(path, std::ios::binary);
    file.write("PAR1", 4);
    std::vector<RowGroup> groups;
    Scratch scratch;
    for (auto row = first; row < last;) {
        const auto count = bounded_group(table, schema, row, std::min(row_group_rows, last - row));
        RowGroup group{ {}, static_cast<int64_t>(count) };
        for (size_t column = 0; column < schema.size(); ++column) {
            const auto chunk = write_chunk(file, table.columns[column], schema[column], row, count,
                                           scratch);
            group.columns[column] = chunk;
            group.uncompressed += chunk.uncompressed;
        }
        groups.push_back(group);
        row += count;
    }
    write_footer(file, schema, last - first, groups);
    file.close();
}

inline void write(const OutputView& output, const fs::path& destination, unsigned threads) {
    struct Part {
        const Table* table;
        std::span<const Field> schema;
        size_t first, last;
        fs::path staging, destination;
    };

    std::vector<Part> parts;
    auto add = [&](const Table& table, std::span<const Field> schema,
                   const fs::path& path, unsigned workers) {
        fs::create_directories(path);
        const auto rows = static_cast<size_t>(table.rows);
        const auto count = std::max<size_t>(1, std::min<size_t>(workers, rows / 262'144));
        const auto per_part = ((rows + count * 8 - 1) / (count * 8)) * 8;
        for (size_t index = 0; index < count; ++index) {
            const auto name = index == 0 ? std::string("part-0.parquet")
                : "part-native-" + std::to_string(index) + ".parquet";
            parts.push_back({&table, schema, index * per_part, std::min((index + 1) * per_part, rows),
                             path / ("." + name + "." + std::to_string(getpid()) + ".tmp"),
                             path / name});
        }
    };
    add(output.users, user_schema, destination / "user_daily", threads);
    add(output.campaigns, campaign_schema, destination / "campaign_daily", 1);

    parallel_for(parts.size(), threads, [&](size_t index, unsigned) {
        const auto& part = parts[index];
        write_file(*part.table, part.schema, part.first, part.last, part.staging);
    });
    for (const auto& part : parts) {
        fs::rename(part.staging, part.destination);
    }

    for (const auto* name : {"user_daily", "campaign_daily"}) {
        for (const auto& entry : fs::directory_iterator(destination / name)) {
            const auto filename = entry.path().filename().native();
            if (!filename.starts_with("part-native-") || !filename.ends_with(".parquet")) {
                continue;
            }
            const auto index = std::string_view(filename).substr(12, filename.size() - 20);
            if (!index.empty() && std::all_of(index.begin(), index.end(), [](char c) {
                    return c >= '0' && c <= '9';
                }) && std::none_of(parts.begin(), parts.end(), [&](const Part& part) {
                    return part.destination == entry.path();
                })) {
                fs::remove(entry.path());
            }
        }
    }
}

}  // namespace shopping::parquet_output

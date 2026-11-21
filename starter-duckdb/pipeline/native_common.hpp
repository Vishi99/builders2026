#pragma once

#include "native_input.hpp"

#include <atomic>
#include <cstdio>
#include <deque>
#include <exception>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace shopping {

namespace fs = std::filesystem;

inline constexpr int64_t day_ms = 86'400'000;
inline constexpr int64_t week_ms = 7 * day_ms;
inline constexpr size_t partition_count = 256;

class StageTimer {
public:
    void report(const char* stage) {
        const auto end = std::chrono::steady_clock::now();
        const std::chrono::duration<double> elapsed = end - start_;
        std::fprintf(stderr, "%s: %.3fs\n", stage, elapsed.count());
        start_ = end;
    }

private:
    std::chrono::steady_clock::time_point start_ = std::chrono::steady_clock::now();
};

inline int32_t day_number(int64_t ts) {
    return static_cast<int32_t>(ts / day_ms - (ts % day_ms < 0));
}

inline uint64_t hash_id(uint64_t key) {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53ULL;
    return key ^ (key >> 33);
}

inline size_t bucket_for_key(uint64_t key) {
    return (hash_id(key) >> 32) % partition_count;
}

template<class Work>
void parallel_for(size_t count, unsigned threads, Work&& work) {
    threads = std::min<size_t>(threads, count);
    if (threads <= 1) {
        for (size_t index = 0; index < count; ++index) {
            work(index, 0);
        }
        return;
    }

    std::atomic<size_t> next{0};
    std::mutex failure_mutex;
    std::exception_ptr failure;
    auto run = [&](unsigned worker) {
        try {
            for (size_t index; (index = next.fetch_add(1, std::memory_order_relaxed)) < count;) {
                work(index, worker);
            }
        } catch (...) {
            next.store(count, std::memory_order_relaxed);
            std::lock_guard lock(failure_mutex);
            if (!failure) {
                failure = std::current_exception();
            }
        }
    };

    {
        std::vector<std::jthread> workers;
        workers.reserve(threads - 1);
        for (unsigned worker = 1; worker < threads; ++worker) {
            workers.emplace_back(run, worker);
        }
        run(0);
    }
    if (failure) {
        std::rethrow_exception(failure);
    }
}

template<size_t Width, class OnRow>
void scan_shards(const fs::path& folder, unsigned threads, OnRow&& on_row) {
    constexpr size_t chunk_bytes = 4 << 20;
    struct Chunk {
        size_t file, begin, end;
    };
    std::vector<input::MappedFile> files;
    std::vector<Chunk> chunks;
    if (!fs::exists(folder)) {
        return;
    }
    for (const auto& entry : fs::directory_iterator(folder)) {
        if (!entry.path().filename().native().ends_with(".csv")) {
            continue;
        }
        const size_t index = files.size();
        files.emplace_back(entry.path().c_str());
        const size_t size = files.back().size();
        for (size_t begin = 0; begin < size; begin += chunk_bytes) {
            chunks.push_back({index, begin, std::min(size, begin + chunk_bytes)});
        }
    }
    parallel_for(chunks.size(), threads, [&](size_t index, unsigned worker) {
        const auto [file, begin, end] = chunks[index];
        input::for_each_row<Width>(files[file], begin, end, [&](const auto& row) {
            on_row(row, worker);
        });
    });
}

struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view key) const noexcept {
        return std::hash<std::string_view>{}(key);
    }
};

using StringIds = std::unordered_map<std::string, uint32_t, StringHash, std::equal_to<>>;

// Short ASCII IDs fit in a word whose order matches their byte order.
// All other IDs are interned exactly in a separate key space.
class IdKeys {
    static constexpr uint64_t interned = 1ULL << 63;

public:
    uint64_t key(std::string_view text) const {
        if (text.size() > 9) {
            return intern(text);
        }
#if defined(__AVX2__) && defined(__BMI2__)
        if (text.size() >= 8) {
            uint64_t prefix;
            std::memcpy(&prefix, text.data(), sizeof(prefix));
            const auto last = text.size() == 9 ? static_cast<unsigned char>(text.back()) : 0;
            constexpr uint64_t high_bits = 0x8080808080808080ULL;
            constexpr uint64_t byte_ones = 0x0101010101010101ULL;
            // A set high bit after subtracting one flags an ASCII NUL byte.
            if (((prefix | (prefix - byte_ones)) & high_bits) == 0 &&
                (text.size() == 8 || (last > 0 && last < 128))) {
                const uint64_t packed = _pext_u64(__builtin_bswap64(prefix), ~high_bits);
                return (packed << 7) | last;
            }
            return intern(text);
        }
#endif
        uint64_t value = 0;
        for (unsigned char byte : text) {
            if (byte == 0 || byte >= 128) {
                return intern(text);
            }
            value = (value << 7) | byte;
        }
        return value << ((9 - text.size()) * 7);
    }

    void decode(uint64_t key, std::string& text) const {
        if (key & interned) {
            std::lock_guard lock(mutex_);
            text.assign(names_[key & ~interned]);
            return;
        }
        char buffer[9];
        size_t length = 0;
        for (int shift = 56; shift >= 0; shift -= 7) {
            const auto digit = (key >> shift) & 127;
            if (digit == 0) {
                break;
            }
            buffer[length++] = static_cast<char>(digit);
        }
        text.assign(buffer, length);
    }

    bool less(uint64_t a, uint64_t b) const {
        if (((a | b) & interned) == 0) {
            return a < b;
        }
        std::string left, right;
        decode(a, left);
        decode(b, right);
        return left < right;
    }

private:
    uint64_t intern(std::string_view text) const {
        std::lock_guard lock(mutex_);
        if (const auto it = keys_.find(text); it != keys_.end()) {
            return it->second;
        }
        const uint64_t key = interned | names_.size();
        keys_.emplace(names_.emplace_back(text), key);
        return key;
    }

    mutable std::mutex mutex_;
    mutable std::deque<std::string> names_;
    mutable std::unordered_map<std::string_view, uint64_t, StringHash> keys_;
};

class IdMap {
    struct Entry {
        uint64_t key = 0;
        uint64_t value = 0;
    };

public:
    uint64_t get(uint64_t key) const {
        return entries_[slot(key)].value;
    }

    void insert(uint64_t key, uint64_t value) {
        if ((size_ + 1) * 3 >= entries_.size() * 2) {
            auto old = std::move(entries_);
            entries_.resize(old.size() * 2);
            for (const auto& entry : old) {
                if (entry.key != 0) {
                    entries_[slot(entry.key)] = entry;
                }
            }
        }
        entries_[slot(key)] = {key, value};
        ++size_;
    }

    template<class Resolve>
    void resolve_values(Resolve&& resolve) {
        for (auto& entry : entries_) {
            if (entry.key != 0) {
                entry.value = resolve(entry.value);
            }
        }
    }

private:
    size_t slot(uint64_t key) const {
        const size_t mask = entries_.size() - 1;
        size_t index = hash_id(key) & mask;
        while (entries_[index].key != 0 && entries_[index].key != key) {
            index = (index + 1) & mask;
        }
        return index;
    }

    std::vector<Entry> entries_ = std::vector<Entry>(16);
    size_t size_ = 0;
};

class Identities {
    struct Node {
        uint64_t minimum;
        uint32_t parent;
        uint32_t size;
    };

public:
    Identities(const fs::path& links, const IdKeys& keys) : keys_(keys) {
        std::vector<Node> nodes;
        auto node_for = [&](uint64_t key) {
            if (const auto stored = aliases_.get(key)) {
                return static_cast<uint32_t>(stored - 1);
            }
            const auto at = static_cast<uint32_t>(nodes.size());
            nodes.push_back({key, at, 1});
            aliases_.insert(key, uint64_t{at} + 1);
            return at;
        };
        auto root = [&](uint32_t at) {
            while (nodes[at].parent != at) {
                nodes[at].parent = nodes[nodes[at].parent].parent;
                at = nodes[at].parent;
            }
            return at;
        };

        scan_shards<2>(links, 1, [&](const auto& row, unsigned) {
            if (row[0].empty() || row[1].empty() || row[0] == row[1]) {
                return;
            }
            auto a = root(node_for(keys_.key(row[0])));
            auto b = root(node_for(keys_.key(row[1])));
            if (a == b) {
                return;
            }
            if (nodes[a].size < nodes[b].size) {
                std::swap(a, b);
            }
            nodes[b].parent = a;
            nodes[a].size += nodes[b].size;
            if (keys_.less(nodes[b].minimum, nodes[a].minimum)) {
                nodes[a].minimum = nodes[b].minimum;
            }
        });
        aliases_.resolve_values([&](uint64_t index) { return nodes[root(index - 1)].minimum; });
    }

    uint64_t person(std::string_view user) const {
        const uint64_t key = keys_.key(user);
        const uint64_t canonical = aliases_.get(key);
        return canonical == 0 ? key : canonical;
    }

private:
    const IdKeys& keys_;
    IdMap aliases_;
};

}  // namespace shopping

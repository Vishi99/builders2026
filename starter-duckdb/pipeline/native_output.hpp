#pragma once

#include "native_events.hpp"
#include "native_attribution.hpp"

#include <cstring>

namespace shopping {

struct Buffer {
    const void* address = nullptr;
    int64_t size = 0;
};

struct Column {
    Buffer validity, offsets, values;
};

struct Table {
    int64_t rows;
    Column columns[7];
};

struct OutputView {
    Table users;
    Table campaigns;
};

template<class Value>
Buffer buffer(const std::vector<Value>& values) {
    return {values.data(), static_cast<int64_t>(values.size() * sizeof(Value))};
}

struct TextColumn {
    std::vector<int64_t> offsets{0};
    std::vector<char> text;

    void append(std::string_view value) {
        text.insert(text.end(), value.begin(), value.end());
        offsets.push_back(text.size());
    }

    Column view(Buffer validity = {}) const {
        return {validity, buffer(offsets), buffer(text)};
    }
};

struct Campaigns {
    std::vector<std::string> names{"organic"};
    std::vector<std::string> channels{"organic"};
    StringIds ids;

    explicit Campaigns(const fs::path& path) {
        input::MappedFile file(path.c_str());
        input::for_each_row<2>(file, [&](const auto& row) {
            if (!row[0].empty()) {
                ids.emplace(row[0], names.size());
                names.emplace_back(row[0]);
                channels.emplace_back(row[1]);
            }
        });
    }
};

struct UserColumns {
    TextColumn users, categories;
    std::vector<int32_t> days;
    std::vector<uint32_t> sessions, events;
    std::vector<int64_t> revenue;
    std::vector<uint8_t> category_present;

    void fill(const std::vector<std::vector<UserDay>>& partitions, const IdKeys& keys,
              const Products& products, unsigned threads) {
        struct Piece {
            std::vector<char> users, categories;
            size_t first_row = 0;
            size_t user_start = 0;
            size_t category_start = 0;
            uint8_t first_validity = 0;
            uint8_t last_validity = 0;
        };
        std::vector<Piece> pieces(partitions.size());
        size_t count = 0;
        for (size_t index = 0; index < partitions.size(); ++index) {
            pieces[index].first_row = count;
            count += partitions[index].size();
        }
        days.resize(count);
        sessions.resize(count);
        events.resize(count);
        revenue.resize(count);
        users.offsets.resize(count + 1);
        categories.offsets.resize(count + 1);
        category_present.resize((count + 7) / 8);
        const auto workers = std::min<size_t>(threads, std::max<size_t>(1, count / 32'768));

        parallel_for(partitions.size(), workers, [&](size_t index, unsigned) {
            auto& piece = pieces[index];
            const auto& partition = partitions[index];
            piece.users.reserve(partition.size() * 9);
            piece.categories.reserve(partition.size());
            size_t row_index = piece.first_row;
            uint64_t previous = 0;
            uint8_t validity = 0;
            std::string name;
            for (const auto& row : partition) {
                if (row.person != previous) {
                    keys.decode(row.person, name);
                    previous = row.person;
                }
                piece.users.insert(piece.users.end(), name.begin(), name.end());
                users.offsets[row_index + 1] = piece.users.size();

                const auto& category = products.categories[row.category];
                piece.categories.insert(piece.categories.end(), category.begin(), category.end());
                categories.offsets[row_index + 1] = piece.categories.size();
                validity |= static_cast<uint8_t>((row.category != 0) << (row_index % 8));
                if (row_index % 8 == 7) {
                    // A byte wholly within this partition needs no merge.
                    if (row_index - piece.first_row >= 7) {
                        category_present[row_index / 8] = validity;
                    } else {
                        piece.first_validity = validity;
                    }
                    validity = 0;
                }

                days[row_index] = row.day;
                sessions[row_index] = row.sessions;
                events[row_index] = row.events;
                revenue[row_index] = row.revenue;
                ++row_index;
            }
            piece.last_validity = validity;
        });

        size_t user_bytes = 0;
        size_t category_bytes = 0;
        for (size_t index = 0; index < pieces.size(); ++index) {
            auto& piece = pieces[index];
            piece.user_start = user_bytes;
            piece.category_start = category_bytes;
            user_bytes += piece.users.size();
            category_bytes += piece.categories.size();
            if (piece.first_validity != 0) {
                category_present[piece.first_row / 8] |= piece.first_validity;
            }
            if (piece.last_validity != 0) {
                const size_t next_row = piece.first_row + partitions[index].size();
                category_present[next_row / 8] |= piece.last_validity;
            }
        }
        users.text.resize(user_bytes);
        categories.text.resize(category_bytes);

        parallel_for(partitions.size(), workers, [&](size_t index, unsigned) {
            const auto& piece = pieces[index];
            if (!piece.users.empty()) {
                std::memcpy(users.text.data() + piece.user_start, piece.users.data(), piece.users.size());
            }
            if (!piece.categories.empty()) {
                std::memcpy(categories.text.data() + piece.category_start,
                            piece.categories.data(), piece.categories.size());
            }
            const size_t last = piece.first_row + partitions[index].size();
            for (size_t row = piece.first_row; row < last; ++row) {
                users.offsets[row + 1] += piece.user_start;
                categories.offsets[row + 1] += piece.category_start;
            }
        });
    }

    Table view() const {
        return {static_cast<int64_t>(days.size()),
                {users.view(), {{}, {}, buffer(days)}, {{}, {}, buffer(sessions)},
                 {{}, {}, buffer(events)}, {{}, {}, buffer(revenue)},
                 categories.view(buffer(category_present))}};
    }
};

struct CampaignColumns {
    TextColumn names, channels;
    std::vector<int32_t> days;
    std::vector<int64_t> sessions, purchasing, revenue, assisted;
    std::vector<uint8_t> channel_present;

    void fill(const std::vector<CampaignDay>& rows, const Campaigns& campaigns) {
        channel_present.resize((rows.size() + 7) / 8);
        for (const auto& row : rows) {
            const auto& channel = campaigns.channels[row.campaign];
            if (!channel.empty()) {
                channel_present[days.size() / 8] |= uint8_t{1} << (days.size() % 8);
            }
            names.append(campaigns.names[row.campaign]);
            channels.append(channel);
            days.push_back(row.day);
            sessions.push_back(row.sessions);
            purchasing.push_back(row.purchasing_sessions);
            revenue.push_back(row.revenue_cents);
            assisted.push_back(row.assisted_sessions);
        }
    }

    Table view() const {
        return {static_cast<int64_t>(days.size()),
                {names.view(), channels.view(buffer(channel_present)), {{}, {}, buffer(days)},
                 {{}, {}, buffer(sessions)}, {{}, {}, buffer(purchasing)},
                 {{}, {}, buffer(revenue)}, {{}, {}, buffer(assisted)}}};
    }
};

struct Output {
    UserColumns users;
    CampaignColumns campaigns;
};

}  // namespace shopping

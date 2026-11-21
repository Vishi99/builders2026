#include "native_events.hpp"
#include "native_attribution.hpp"

#include <cstdio>
#include <memory>

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
              const Products& products) {
        size_t count = 0;
        for (const auto& partition : partitions) {
            count += partition.size();
        }
        days.reserve(count);
        sessions.reserve(count);
        events.reserve(count);
        revenue.reserve(count);
        users.offsets.reserve(count + 1);
        categories.offsets.reserve(count + 1);
        category_present.resize((count + 7) / 8);

        uint64_t previous = 0;
        std::string name;
        for (const auto& partition : partitions) {
            for (const auto& row : partition) {
                if (row.person != previous) {
                    keys.decode(row.person, name);
                    previous = row.person;
                }
                if (row.category != 0) {
                    category_present[days.size() / 8] |= uint8_t{1} << (days.size() % 8);
                }
                users.append(name);
                days.push_back(row.day);
                sessions.push_back(row.sessions);
                events.push_back(row.events);
                revenue.push_back(row.revenue);
                categories.append(products.categories[row.category]);
            }
        }
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

extern "C" shopping::Output* shopping_run(const char* data_dir, const char* window_start,
                                          const char* window_end,
                                          unsigned threads, shopping::OutputView* view,
                                          char* error, size_t error_size) noexcept {
    using namespace shopping;
    try {
        const auto start = input::timestamp_ms(window_start);
        const auto end = input::timestamp_ms(window_end);
        if (threads == 0 || start == input::invalid_timestamp || start >= end) {
            throw std::invalid_argument("Expected positive NCPU and an increasing dataset window");
        }
        const fs::path data(data_dir);
        auto result = std::make_unique<Output>();
        StageTimer timer;
        IdKeys user_keys;
        const Identities identities(data / "links", user_keys);
        const Products products(data / "products.csv");
        const Campaigns campaigns(data / "campaigns.csv");
        timer.report("Resolve identities and load dimensions");
        const auto events = process_events(data, start, end, identities, products, threads, timer);
        IdKeys click_keys;
        const auto credited = attribute(data, start, end, events.sessions, identities, click_keys,
                                        campaigns.ids, threads);
        timer.report("Parse clicks and attribute sessions");
        result->users.fill(events.days, user_keys, products);
        result->campaigns.fill(credited, campaigns);
        timer.report("Assemble output columns");
        *view = {result->users.view(), result->campaigns.view()};
        return result.release();
    } catch (const std::exception& failure) {
        std::snprintf(error, error_size, "%s", failure.what());
        return nullptr;
    }
}

extern "C" void shopping_free(shopping::Output* output) noexcept {
    delete output;
}

#include "native_output.hpp"
#include "native_packed_events.hpp"

#include <cstdio>
#include <future>
#include <memory>

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
        const Products products(data / "products.csv");
        const Campaigns campaigns(data / "campaigns.csv");
        timer.report("Load product and campaign dimensions");

        const bool can_pack = threads <= alignof(packed_events::Ticket) &&
                              compact_events_fit(start, end, products);
        const bool overlap_links = can_pack && threads > 1;
        auto linked = std::async(overlap_links ? std::launch::async : std::launch::deferred, [&] {
            return Identities(data / "links", user_keys, overlap_links ? 1 : threads);
        });
        std::optional<packed_events::Input> packed;
        if (can_pack) {
            const unsigned scan_threads = threads - overlap_links;
            packed = packed_events::scan(data, start, end, user_keys, products, scan_threads);
            timer.report("Parse and partition raw events");
            if (packed) {
                const unsigned dedup_threads = linked.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::timeout ? scan_threads : threads;
                packed_events::deduplicate(*packed, dedup_threads);
                timer.report("Deduplicate event tickets");
            }
        }
        IdKeys click_keys;
        Partitioned<packed_events::CompactClick> raw_clicks;
        const auto click_time_base = start - week_ms;
        if (packed && static_cast<uint64_t>(end) - static_cast<uint64_t>(click_time_base) <=
            (uint64_t{1} << 32)) {
            raw_clicks = scan_click_records<uint32_t>(
                data, start, end, click_keys, campaigns.ids, threads, click_time_base,
                [&](std::string_view user, unsigned) { return user_keys.key(user); });
            timer.report("Parse and partition raw clicks");
        }
        const Identities identities = linked.get();
        timer.report("Finish linked ID resolution");
        EventTotals events;
        if (packed) {
            events = packed_events::make_sessions(*packed, identities, start,
                                                  products.categories.size(), threads, timer, raw_clicks);
            timer.report("Merge bins and build user sessions");
        } else {
            events = process_events(data, start, end, identities, products, threads, timer);
        }
        const bool clicks_prepared = !raw_clicks.empty();
        const auto credited = clicks_prepared
            ? attribute_partitioned(start, end, events.sessions, std::move(raw_clicks), click_keys,
                                    campaigns.ids.size(), threads, click_time_base)
            : attribute(data, start, end, events.sessions, identities, click_keys, campaigns.ids, threads);
        timer.report(clicks_prepared ? "Attribute sessions" : "Parse clicks and attribute sessions");
        result->users.fill(events.days, user_keys, products, threads);
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

#pragma once

#include "native_common.hpp"

#include <bit>

namespace shopping {

inline constexpr uint32_t session_gap_ms = 1'800'000;

class CategoryRevenue {
public:
    explicit CategoryRevenue(size_t count) : revenue_(count) {}

    void add(uint32_t category, uint32_t price) {
        auto& revenue = revenue_[category];
        if (revenue == 0) {
            purchased_.push_back(category);
        }
        revenue += price;
    }

    uint32_t winner() {
        uint32_t best = 0;
        int64_t best_revenue = 0;
        for (const auto category : purchased_) {
            auto& revenue = revenue_[category];
            if (revenue > best_revenue || (revenue == best_revenue && category < best)) {
                best = category;
                best_revenue = revenue;
            }
            revenue = 0;
        }
        purchased_.clear();
        return best;
    }

private:
    std::vector<int64_t> revenue_;
    std::vector<uint32_t> purchased_;
};

namespace session_bins {

struct Bin {
    uint64_t person;
    uint32_t first, last;
    uint32_t count = 0;
    uint32_t purchase = 0;
};

struct Purchase {
    uint32_t price;
    uint32_t category;
    uint32_t next;
};

class Bins {
public:
    explicit Bins(size_t events)
        : slots_(std::bit_ceil(std::max<size_t>(16, events / 4)), 0) {
        values.reserve(events / 5);
        purchases.reserve(events / 32);
    }

    void add(uint64_t person, uint32_t offset, uint32_t price, uint32_t category) {
        const uint32_t number = offset / session_gap_ms;
        size_t index = slot(person, number);
        if (slots_[index] == 0) {
            if ((values.size() + 1) * 3 > slots_.size() * 2) {
                grow();
                index = slot(person, number);
            }
            values.push_back({person, offset, offset});
            slots_[index] = static_cast<uint32_t>(values.size());
        }
        auto& bin = values[slots_[index] - 1];
        bin.first = std::min(bin.first, offset);
        bin.last = std::max(bin.last, offset);
        ++bin.count;
        if (price != 0) {
            purchases.push_back({price, category, bin.purchase});
            bin.purchase = static_cast<uint32_t>(purchases.size());
        }
    }

    std::vector<Bin> values;
    std::vector<Purchase> purchases;

private:
    size_t slot(uint64_t person, uint32_t number) const {
        const auto mask = slots_.size() - 1;
        size_t index = hash_id(person + number * 0x9e3779b97f4a7c15ULL) & mask;
        while (slots_[index] != 0) {
            const auto& candidate = values[slots_[index] - 1];
            if (candidate.person == person && candidate.first / session_gap_ms == number) {
                break;
            }
            index = (index + 1) & mask;
        }
        return index;
    }

    void grow() {
        slots_.assign(slots_.size() * 2, 0);
        for (size_t index = 0; index < values.size(); ++index) {
            const auto& bin = values[index];
            slots_[slot(bin.person, bin.first / session_gap_ms)] = static_cast<uint32_t>(index + 1);
        }
    }

    std::vector<uint32_t> slots_;
};

template<class Session, class UserDay>
void summarize(Bins& bins, int64_t start, size_t category_count,
               std::vector<Session>& sessions, std::vector<UserDay>& days) {
    // Events in a 30-minute bin have no gap greater than 30 minutes.
    // For gaps between bins, only the previous maximum and next minimum matter.
    auto& values = bins.values;
    std::sort(values.begin(), values.end(), [](const Bin& left, const Bin& right) {
        return left.person != right.person ? left.person < right.person : left.first < right.first;
    });
    CategoryRevenue category_revenue(category_count);
    size_t index = 0;
    while (index < values.size()) {
        const auto person = values[index].person;
        const auto day = day_number(start + values[index].first);
        UserDay totals{person, 0, day, 0, 0, 0};
        do {
            Session session{person, start + values[index].first, 0};
            uint32_t last;
            do {
                const auto& bin = values[index++];
                last = bin.last;
                totals.events += bin.count;
                for (uint32_t at = bin.purchase; at != 0;) {
                    const auto& purchase = bins.purchases[at - 1];
                    session.revenue += purchase.price;
                    category_revenue.add(purchase.category, purchase.price);
                    at = purchase.next;
                }
            } while (index < values.size() && values[index].person == person &&
                     values[index].first - last <= session_gap_ms);
            sessions.push_back(session);
            ++totals.sessions;
            totals.revenue += session.revenue;
        } while (index < values.size() && values[index].person == person &&
                 day_number(start + values[index].first) == day);

        totals.category = category_revenue.winner();
        days.push_back(totals);
    }
}

}  // namespace session_bins
}  // namespace shopping

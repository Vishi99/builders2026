#include "native_parquet.hpp"

#include <cstdlib>
#include <iostream>

#include <nlohmann/json.hpp>

extern "C" shopping::Output* shopping_run(const char*, const char*, const char*, unsigned,
                                          shopping::OutputView*, char*, size_t) noexcept;
extern "C" void shopping_free(shopping::Output*) noexcept;

const char* environment(const char* name, const char* fallback) {
    const char* value = std::getenv(name);
    return value != nullptr && *value != '\0' ? value : fallback;
}

int main() {
    using namespace shopping;
    StageTimer timer;
    try {
        unsigned threads;
        if (const char* count = std::getenv("NCPU")) {
            const auto end = count + std::strlen(count);
            const auto parsed = std::from_chars(count, end, threads);
            if (parsed.ec != std::errc{} || parsed.ptr != end || threads == 0) {
                throw std::invalid_argument("NCPU must be a positive integer");
            }
        } else {
            threads = std::max(1u, std::thread::hardware_concurrency());
        }
        const fs::path data(environment("DATA_DIR", "/data"));
        const fs::path output(environment("OUTPUT_DIR", "/output"));
        std::ifstream manifest_file(data / "manifest.json");
        const auto manifest = nlohmann::json::parse(manifest_file);
        const auto start = manifest.at("window_start").get<std::string>();
        const auto end = manifest.at("window_end").get<std::string>();

        OutputView view{};
        char error[1024]{};
        std::unique_ptr<Output, decltype(&shopping_free)> result(
            shopping_run(data.c_str(), start.c_str(), end.c_str(), threads, &view, error, sizeof(error)),
            shopping_free);
        if (!result) {
            throw std::runtime_error(error);
        }

        StageTimer output_timer;
        parquet_output::write(view, output, threads);
        output_timer.report("Write native Parquet");
        result.reset();
        timer.report("Total native executable");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

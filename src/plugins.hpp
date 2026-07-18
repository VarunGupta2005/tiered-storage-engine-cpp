#pragma once
#include "interfaces.hpp"
#include "bloom_filter.hpp"
#include <unordered_map>
#include <iostream>

// CQRS Read Model: KVCachePlugin
class KVCachePlugin : public IProjector {
private:
    std::unordered_map<std::string, std::string> cache;
    BloomFilter filter;

public:
    KVCachePlugin() : filter(1000) {}

    void onEvent(const Event& event) override {
        // Only care about Key-Value updates
        if (event.topic == "KV_UPDATE") {
            cache[event.key] = event.payload;
            filter.add(event.key);
        }
    }

    std::string get(const std::string& key) {
        // O(1) missing key rejection
        if (!filter.mightContain(key)) {
            return "NOT_FOUND (Bloom Filter rejected instantly)";
        }
        auto it = cache.find(key);
        return it != cache.end() ? it->second : "NOT_FOUND";
    }
};

// CQRS Read Model: AnalyticsPlugin
class AnalyticsPlugin : public IProjector {
private:
    std::unordered_map<std::string, int> updateCounts;

public:
    void onEvent(const Event& event) override {
        if (event.topic == "KV_UPDATE") {
            updateCounts[event.key]++;
            std::cout << "[Analytics] Key '" << event.key << "' updated " << updateCounts[event.key] << " times.\n";
        }
    }
};

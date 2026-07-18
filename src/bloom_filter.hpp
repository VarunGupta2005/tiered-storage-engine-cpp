#pragma once
#include <vector>
#include <string>
#include <functional>

// Simple Bloom Filter to reject missing keys in O(1) time
// Applies space-time tradeoff optimization pattern
class BloomFilter {
private:
    std::vector<bool> bits;
    size_t size;

    size_t hash1(const std::string& key) const {
        return std::hash<std::string>{}(key) % size;
    }

    size_t hash2(const std::string& key) const {
        size_t h = 0;
        for (char c : key) h = (h * 31 + c) % size;
        return h;
    }

public:
    explicit BloomFilter(size_t s = 1000) : size(s) {
        bits.resize(size, false);
    }

    void add(const std::string& key) {
        bits[hash1(key)] = true;
        bits[hash2(key)] = true;
    }

    bool mightContain(const std::string& key) const {
        return bits[hash1(key)] && bits[hash2(key)];
    }
};

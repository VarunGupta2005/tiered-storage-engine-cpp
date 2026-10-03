#include "engine.hpp"
#include "storage.hpp"
#include "plugins.hpp"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    std::cout << "--- Starting Hybrid Event-Streaming KV Store ---\n";

    // 1. Setup Storage and Broker
    auto log = std::make_shared<DiskEventLog>("events.log");
    EventBroker broker(log);

    // 2. Setup Plugins (CQRS Read-Models)
    auto kvCache = std::make_shared<KVCachePlugin>();
    auto analytics = std::make_shared<AnalyticsPlugin>();
    
    broker.registerPlugin(kvCache);
    broker.registerPlugin(analytics);

    // 3. Background Consumer Thread using encapsulated blocking call
    std::thread consumer([&broker, &kvCache]() {
        std::cout << "[Consumer] Thread started. Sleeping until events arrive...\n";
        
        // Block the thread cleanly without burning CPU or causing deadlocks
        auto events = broker.consumeBlocking("KV_UPDATE");

        std::cout << "[Consumer] Woke up! Checking state in O(1)...\n";
        std::cout << "State of 'user_123': " << kvCache->get("user_123") << "\n";
        std::cout << "State of 'invalid_user': " << kvCache->get("invalid_user") << "\n";
    });

    // Simulate system running before events happen
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 4. Producer Thread publishing events
    std::cout << "[Producer] Publishing Uber/E-commerce Events...\n";
    broker.publish({1, "KV_UPDATE", "user_123", "{\"status\":\"searching_ride\"}"});
    
    // Slight delay to simulate real-world event streaming
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    broker.publish({2, "KV_UPDATE", "user_123", "{\"status\":\"ride_found\", \"driver\":\"john\"}"});

    consumer.join();

    std::cout << "--- Demo Completed ---\n";
    return 0;
}
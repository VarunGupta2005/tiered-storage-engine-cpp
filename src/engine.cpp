#include "engine.hpp"

EventBroker::EventBroker(std::shared_ptr<IEventLog> log) : eventLog(std::move(log)) {
    // Recover existing events from the Append-Only Disk Log
    auto recovered = eventLog->recover();
    for (const auto& e : recovered) {
        streams[e.topic].push_back(e);
    }
}

void EventBroker::publish(const Event& event) {
    // 1. Write to durable append-only log
    eventLog->append(event);

    {
        // 2. Update RAM streams (Exclusive Write Lock)
        std::unique_lock<std::shared_mutex> lock(rwMutex);
        streams[event.topic].push_back(event);
    }

    // 3. Broadcast to all CQRS read-models
    for (auto& plugin : plugins) {
        plugin->onEvent(event);
    }

    // 4. Wake up any sleeping consumers (0% CPU spin)
    cv.notify_all();
}

void EventBroker::registerPlugin(std::shared_ptr<IProjector> plugin) {
    plugins.push_back(plugin);
    // Replay past events to initialize the read-model
    std::shared_lock<std::shared_mutex> lock(rwMutex);
    for (const auto& pair : streams) {
        for (const auto& e : pair.second) {
            plugin->onEvent(e);
        }
    }
}

std::vector<Event> EventBroker::consume(const std::string& topic) {
    // Shared Read Lock allowing concurrent reads
    std::shared_lock<std::shared_mutex> lock(rwMutex);
    auto it = streams.find(topic);
    if (it != streams.end()) {
        return it->second;
    }
    return {};
}
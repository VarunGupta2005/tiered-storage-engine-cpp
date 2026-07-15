#pragma once
#include "interfaces.hpp"
#include <vector>
#include <map>
#include <memory>
#include <shared_mutex>
#include <condition_variable>

class EventBroker : public IEventBroker {
private:
    std::shared_ptr<IEventLog> eventLog;
    std::vector<std::shared_ptr<IProjector>> plugins;

    // Ordered map of streams in RAM (Topic -> List of Events)
    std::map<std::string, std::vector<Event>> streams;
    
public:
    // Concurrency primitives
    mutable std::shared_mutex rwMutex;
    std::condition_variable_any cv;

    explicit EventBroker(std::shared_ptr<IEventLog> log);
    ~EventBroker() override = default;

    void publish(const Event& event) override;
    void registerPlugin(std::shared_ptr<IProjector> plugin) override;

    std::vector<Event> consume(const std::string& topic);
};
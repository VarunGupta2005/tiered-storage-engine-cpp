#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include <vector>

// Primary Data Transfer Object (DTO) for our Event-Streaming architecture
struct Event {
    uint64_t id;
    std::string topic;
    std::string key;
    std::string payload;
};

// SOLID: Dependency Inversion Principle - Storage layer contract
class IEventLog {
public:
    virtual ~IEventLog() = default;
    virtual void append(const Event& event) = 0;
    virtual std::vector<Event> recover() = 0;
};

// SOLID: Interface Segregation Principle - CQRS Read Model contract
class IProjector {
public:
    virtual ~IProjector() = default;
    virtual void onEvent(const Event& event) = 0;
};

// Central contract for the Pub/Sub Broker
class IEventBroker {
public:
    virtual ~IEventBroker() = default;
    virtual void publish(const Event& event) = 0;
    virtual void registerPlugin(std::shared_ptr<IProjector> plugin) = 0;
};
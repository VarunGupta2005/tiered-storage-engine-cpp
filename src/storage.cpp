#include "storage.hpp"
#include <iostream>
#include <sstream>

DiskEventLog::DiskEventLog(const std::string& path) : filePath(path) {
    // Open in append mode to ensure blazing-fast sequential writes
    outStream.open(filePath, std::ios::out | std::ios::app);
    if (!outStream.is_open()) {
        throw std::runtime_error("Failed to open event log file: " + filePath);
    }
}

DiskEventLog::~DiskEventLog() {
    if (outStream.is_open()) {
        outStream.close();
    }
}

void DiskEventLog::append(const Event& event) {
    std::lock_guard<std::mutex> lock(fileMutex);
    
    // Serialize the event. Format: ID,Topic,Key,Payload
    outStream << event.id << "," 
              << event.topic << "," 
              << event.key << "," 
              << event.payload << "\n";
    outStream.flush(); // Flush ensures durability
}

std::vector<Event> DiskEventLog::recover() {
    std::lock_guard<std::mutex> lock(fileMutex);
    std::vector<Event> events;
    
    std::ifstream inStream(filePath);
    if (!inStream.is_open()) return events;

    std::string line;
    while (std::getline(inStream, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string idStr, topic, key, payload;
        
        std::getline(ss, idStr, ',');
        std::getline(ss, topic, ',');
        std::getline(ss, key, ',');
        std::getline(ss, payload); // Read remainder into payload

        Event e;
        e.id = std::stoull(idStr);
        e.topic = topic;
        e.key = key;
        e.payload = payload;
        events.push_back(e);
    }
    
    return events;
}
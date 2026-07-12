#pragma once
#include "interfaces.hpp"
#include <fstream>
#include <string>
#include <mutex>

class DiskEventLog : public IEventLog {
private:
    std::string filePath;
    std::ofstream outStream;
    std::mutex fileMutex;

public:
    explicit DiskEventLog(const std::string& path);
    ~DiskEventLog() override;

    void append(const Event& event) override;
    std::vector<Event> recover() override;
};
#include "logger.h"

namespace tvmjns {

Logger::Level Logger::level_ = Logger::INFO;
std::mutex Logger::mutex_;

} // namespace tvmjns

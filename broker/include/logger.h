#ifndef TVMJNS_LOGGER_H
#define TVMJNS_LOGGER_H

#include <string>
#include <mutex>
#include <cstdio>
#include <ctime>
#include <cstdarg>

namespace tvmjns {

class Logger {
public:
    enum Level {
        DEBUG = 0,
        INFO = 1,
        WARN = 2,
        ERROR = 3
    };

    static void set_level(Level level) {
        level_ = level;
    }

    template<typename... Args>
    static void debug(const char* fmt, Args... args) {
        log(DEBUG, fmt, args...);
    }

    template<typename... Args>
    static void info(const char* fmt, Args... args) {
        log(INFO, fmt, args...);
    }

    template<typename... Args>
    static void warn(const char* fmt, Args... args) {
        log(WARN, fmt, args...);
    }

    template<typename... Args>
    static void error(const char* fmt, Args... args) {
        log(ERROR, fmt, args...);
    }

private:
    template<typename... Args>
    static void log(Level level, const char* fmt, Args... args) {
        if (level < level_) return;

        std::lock_guard<std::mutex> lock(mutex_);
        
        time_t now = time(nullptr);
        char time_buf[32];
        strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", localtime(&now));

        const char* level_str[] = {"DEBUG", "INFO", "WARN", "ERROR"};
        fprintf(stderr, "[%s] [%s] ", time_buf, level_str[level]);
        fprintf(stderr, fmt, args...);
        fprintf(stderr, "\n");
        fflush(stderr);
    }

    static Level level_;
    static std::mutex mutex_;
};

} // namespace tvmjns

#endif // TVMJNS_LOGGER_H

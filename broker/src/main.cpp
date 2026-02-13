#include "tcp_server.h"
#include "logger.h"
#include <signal.h>
#include <atomic>

std::atomic<bool> g_shutdown(false);

void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        tvmjns::Logger::info("Received signal %d, shutting down...", sig);
        g_shutdown = true;
    }
}

int main(int argc, char** argv) {
    // Set up signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    tvmjns::Logger::set_level(tvmjns::Logger::INFO);
    tvmjns::Logger::info("TVMJNS Broker starting...");

    uint16_t port = 9090;
    size_t num_threads = 4;

    if (argc > 1) {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }
    if (argc > 2) {
        num_threads = static_cast<size_t>(std::atoi(argv[2]));
    }

    tvmjns::TcpServer server(port, num_threads);
    
    // Start server in a separate thread
    std::thread server_thread([&server]() {
        server.start();
    });

    // Wait for shutdown signal
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    server.stop();
    if (server_thread.joinable()) {
        server_thread.join();
    }

    tvmjns::Logger::info("TVMJNS Broker stopped");
    return 0;
}

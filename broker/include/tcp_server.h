#ifndef TVMJNS_TCP_SERVER_H
#define TVMJNS_TCP_SERVER_H

#include "thread_pool.h"
#include "../common/protocol/protocol.h"
#include <string>
#include <atomic>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace tvmjns {

class TcpServer {
public:
    TcpServer(uint16_t port, size_t num_threads = 4);
    ~TcpServer();

    // Disable copy and move
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    void start();
    void stop();

private:
    void accept_loop();
    void handle_client(int client_fd);
    bool set_nonblocking(int fd);
    bool read_message(int fd, protocol::MessageHeader& header, std::vector<char>& payload);
    bool write_message(int fd, const protocol::MessageHeader& header, const char* payload);

    uint16_t port_;
    int server_fd_;
    int epoll_fd_;
    std::atomic<bool> running_;
    std::unique_ptr<ThreadPool> thread_pool_;
    
    // Track active connections
    std::mutex clients_mutex_;
    std::unordered_map<int, std::string> active_clients_;
};

} // namespace tvmjns

#endif // TVMJNS_TCP_SERVER_H

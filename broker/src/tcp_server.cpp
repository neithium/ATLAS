#include "tcp_server.h"
#include "logger.h"
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <errno.h>
#include <vector>

namespace tvmjns {

TcpServer::TcpServer(uint16_t port, size_t num_threads)
    : port_(port)
    , server_fd_(-1)
    , epoll_fd_(-1)
    , running_(false)
    , thread_pool_(std::make_unique<ThreadPool>(num_threads)) {
    Logger::info("TcpServer created on port %d with %zu threads", port, num_threads);
}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        Logger::error("fcntl F_GETFL failed: %s", strerror(errno));
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        Logger::error("fcntl F_SETFL failed: %s", strerror(errno));
        return false;
    }
    return true;
}

void TcpServer::start() {
    if (running_) {
        Logger::warn("TcpServer already running");
        return;
    }

    // Create socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        Logger::error("socket() failed: %s", strerror(errno));
        return;
    }

    // Set SO_REUSEADDR
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        Logger::error("setsockopt SO_REUSEADDR failed: %s", strerror(errno));
        close(server_fd_);
        return;
    }

    // Bind
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(server_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        Logger::error("bind() failed: %s", strerror(errno));
        close(server_fd_);
        return;
    }

    // Listen
    if (listen(server_fd_, SOMAXCONN) < 0) {
        Logger::error("listen() failed: %s", strerror(errno));
        close(server_fd_);
        return;
    }

    // Create epoll instance
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        Logger::error("epoll_create1() failed: %s", strerror(errno));
        close(server_fd_);
        return;
    }

    // Set server socket to non-blocking
    if (!set_nonblocking(server_fd_)) {
        close(epoll_fd_);
        close(server_fd_);
        return;
    }

    // Add server socket to epoll
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET; // Edge-triggered
    ev.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) {
        Logger::error("epoll_ctl() failed: %s", strerror(errno));
        close(epoll_fd_);
        close(server_fd_);
        return;
    }

    running_ = true;
    Logger::info("TcpServer started on port %d", port_);
    accept_loop();
}

void TcpServer::accept_loop() {
    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    while (running_) {
        int n = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000); // 1 second timeout
        if (n < 0) {
            if (errno == EINTR) continue;
            Logger::error("epoll_wait() failed: %s", strerror(errno));
            break;
        }

        for (int i = 0; i < n; ++i) {
            if (events[i].data.fd == server_fd_) {
                // Accept new connections
                while (true) {
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    int client_fd = accept(server_fd_, (sockaddr*)&client_addr, &client_len);
                    
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // No more connections
                        }
                        Logger::error("accept() failed: %s", strerror(errno));
                        break;
                    }

                    char client_ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
                    Logger::info("New connection from %s:%d (fd=%d)", 
                                client_ip, ntohs(client_addr.sin_port), client_fd);

                    {
                        std::lock_guard<std::mutex> lock(clients_mutex_);
                        active_clients_[client_fd] = std::string(client_ip) + ":" + 
                                                     std::to_string(ntohs(client_addr.sin_port));
                    }

                    // Handle client in thread pool
                    thread_pool_->enqueue([this, client_fd]() {
                        handle_client(client_fd);
                    });
                }
            }
        }
    }
}

void TcpServer::handle_client(int client_fd) {
    // Set to non-blocking
    set_nonblocking(client_fd);

    Logger::debug("Handling client fd=%d", client_fd);

    while (running_) {
        protocol::MessageHeader header;
        std::vector<char> payload;

        if (!read_message(client_fd, header, payload)) {
            break;
        }

        // Process message based on type
        protocol::MessageType msg_type = static_cast<protocol::MessageType>(header.msg_type);
        
        switch (msg_type) {
            case protocol::MessageType::PING: {
                Logger::debug("Received PING from fd=%d", client_fd);
                protocol::MessageHeader pong_header(protocol::MessageType::PONG, 0);
                write_message(client_fd, pong_header, nullptr);
                break;
            }
            case protocol::MessageType::DATA: {
                Logger::info("Received DATA from fd=%d, size=%u", client_fd, header.payload_size);
                // Echo ACK
                protocol::MessageHeader ack_header(protocol::MessageType::ACK, 0);
                write_message(client_fd, ack_header, nullptr);
                break;
            }
            default:
                Logger::warn("Unknown message type %d from fd=%d", header.msg_type, client_fd);
                break;
        }
    }

    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        active_clients_.erase(client_fd);
    }
    close(client_fd);
    Logger::info("Client fd=%d disconnected", client_fd);
}

bool TcpServer::read_message(int fd, protocol::MessageHeader& header, std::vector<char>& payload) {
    // Read header
    char* header_ptr = reinterpret_cast<char*>(&header);
    size_t header_size = sizeof(protocol::MessageHeader);
    size_t total_read = 0;

    while (total_read < header_size) {
        ssize_t n = read(fd, header_ptr + total_read, header_size - total_read);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000); // Brief sleep for non-blocking
                continue;
            }
            Logger::error("read() header failed: %s", strerror(errno));
            return false;
        }
        if (n == 0) {
            Logger::debug("Connection closed by client");
            return false;
        }
        total_read += n;
    }

    // Validate header
    if (!header.isValid()) {
        Logger::error("Invalid message header, magic=0x%08X", header.magic);
        return false;
    }

    // Read payload if present
    if (header.payload_size > 0) {
        payload.resize(header.payload_size);
        total_read = 0;
        while (total_read < header.payload_size) {
            ssize_t n = read(fd, payload.data() + total_read, header.payload_size - total_read);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    usleep(1000);
                    continue;
                }
                Logger::error("read() payload failed: %s", strerror(errno));
                return false;
            }
            if (n == 0) {
                Logger::error("Unexpected EOF reading payload");
                return false;
            }
            total_read += n;
        }
    }

    return true;
}

bool TcpServer::write_message(int fd, const protocol::MessageHeader& header, const char* payload) {
    // Write header (zero-copy hint: use writev for scatter-gather I/O)
    const char* header_ptr = reinterpret_cast<const char*>(&header);
    size_t header_size = sizeof(protocol::MessageHeader);
    size_t total_written = 0;

    while (total_written < header_size) {
        ssize_t n = write(fd, header_ptr + total_written, header_size - total_written);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(1000);
                continue;
            }
            Logger::error("write() header failed: %s", strerror(errno));
            return false;
        }
        total_written += n;
    }

    // Write payload if present
    if (header.payload_size > 0 && payload != nullptr) {
        total_written = 0;
        while (total_written < header.payload_size) {
            ssize_t n = write(fd, payload + total_written, header.payload_size - total_written);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    usleep(1000);
                    continue;
                }
                Logger::error("write() payload failed: %s", strerror(errno));
                return false;
            }
            total_written += n;
        }
    }

    return true;
}

void TcpServer::stop() {
    if (!running_) return;
    
    Logger::info("TcpServer stopping...");
    running_ = false;

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }

    // Close all client connections
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (const auto& pair : active_clients_) {
            close(pair.first);
        }
        active_clients_.clear();
    }

    Logger::info("TcpServer stopped");
}

} // namespace tvmjns

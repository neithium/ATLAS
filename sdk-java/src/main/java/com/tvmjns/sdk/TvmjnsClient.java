package com.tvmjns.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java client for connecting to TVMJNS broker.
 * Thread-safe producer client that sends binary messages.
 */
public class TvmjnsClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TvmjnsClient.class);

    private final String host;
    private final int port;
    private Socket socket;
    private OutputStream outputStream;
    private InputStream inputStream;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final Object writeLock = new Object();

    public TvmjnsClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connect to the broker.
     */
    public void connect() throws IOException {
        if (connected.get()) {
            logger.warn("Already connected");
            return;
        }

        logger.info("Connecting to {}:{}", host, port);
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true); // Disable Nagle's algorithm for low latency
        socket.setSoTimeout(5000); // 5 second read timeout
        
        outputStream = socket.getOutputStream();
        inputStream = socket.getInputStream();
        connected.set(true);
        
        logger.info("Connected to {}:{}", host, port);
    }

    /**
     * Send a message to the broker.
     * Thread-safe method.
     */
    public void send(Message message) throws IOException {
        if (!connected.get()) {
            throw new IllegalStateException("Not connected");
        }

        byte[] data = message.serialize();
        
        synchronized (writeLock) {
            outputStream.write(data);
            outputStream.flush();
        }
        
        logger.debug("Sent message: {}", message);
    }

    /**
     * Send data message with string payload.
     */
    public void sendData(String data) throws IOException {
        byte[] payload = data.getBytes(StandardCharsets.UTF_8);
        Message message = new Message(Message.MessageType.DATA, payload);
        send(message);
    }

    /**
     * Send data message with byte array payload.
     */
    public void sendData(byte[] data) throws IOException {
        Message message = new Message(Message.MessageType.DATA, data);
        send(message);
    }

    /**
     * Send ping and wait for pong response.
     */
    public boolean ping() throws IOException {
        if (!connected.get()) {
            throw new IllegalStateException("Not connected");
        }

        Message pingMsg = new Message(Message.MessageType.PING);
        send(pingMsg);
        
        // Read response
        byte[] header = new byte[12];
        int bytesRead = inputStream.read(header);
        if (bytesRead != 12) {
            logger.error("Failed to read pong response");
            return false;
        }

        Message response = Message.deserialize(header);
        boolean isPong = response.getType() == Message.MessageType.PONG;
        logger.debug("Ping response: {}", response);
        return isPong;
    }

    /**
     * Disconnect from the broker.
     */
    public void disconnect() {
        if (!connected.getAndSet(false)) {
            return;
        }

        logger.info("Disconnecting from {}:{}", host, port);
        
        try {
            if (outputStream != null) {
                outputStream.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing output stream", e);
        }

        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing input stream", e);
        }

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException e) {
            logger.warn("Error closing socket", e);
        }

        logger.info("Disconnected");
    }

    @Override
    public void close() {
        disconnect();
    }

    public boolean isConnected() {
        return connected.get();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}

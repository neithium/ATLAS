package com.tvmjns.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Example producer application.
 */
public class ExampleProducer {
    private static final Logger logger = LoggerFactory.getLogger(ExampleProducer.class);

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9090;

        if (args.length > 0) {
            host = args[0];
        }
        if (args.length > 1) {
            port = Integer.parseInt(args[1]);
        }

        logger.info("Starting TVMJNS Example Producer");

        try (TvmjnsClient client = new TvmjnsClient(host, port)) {
            // Connect
            client.connect();
            logger.info("Connected to broker at {}:{}", host, port);

            // Test ping
            if (client.ping()) {
                logger.info("Ping successful");
            }

            // Send some data messages
            for (int i = 0; i < 10; i++) {
                String message = String.format("Message %d: Hello from Java!", i);
                client.sendData(message);
                logger.info("Sent: {}", message);
                Thread.sleep(1000); // 1 second delay
            }

            logger.info("Finished sending messages");

        } catch (IOException e) {
            logger.error("IO error", e);
        } catch (InterruptedException e) {
            logger.error("Interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}

"""
Example consumer application demonstrating message consumption.
"""

import logging
import time
import signal
import sys
from tvmjns import TvmjnsConsumer, MessageType

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

shutdown = False


def signal_handler(sig, frame):
    global shutdown
    logger.info("Received signal, shutting down...")
    shutdown = True


def main():
    host = 'localhost'
    port = 9090

    if len(sys.argv) > 1:
        host = sys.argv[1]
    if len(sys.argv) > 2:
        port = int(sys.argv[2])

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting TVMJNS Example Consumer")

    with TvmjnsConsumer(host, port) as consumer:
        logger.info(f"Connected to broker at {host}:{port}")

        # Test ping
        if consumer.ping():
            logger.info("Ping successful")

        # Consume messages
        message_count = 0
        while not shutdown:
            message = consumer.receive()
            if message:
                if message.msg_type == MessageType.DATA:
                    data = message.payload.decode('utf-8', errors='ignore')
                    logger.info(f"Received DATA: {data}")
                    message_count += 1
                elif message.msg_type == MessageType.ACK:
                    logger.debug("Received ACK")
            else:
                time.sleep(0.1)

        logger.info(f"Consumed {message_count} messages")


if __name__ == '__main__':
    main()

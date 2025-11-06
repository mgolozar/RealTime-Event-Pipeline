#!/bin/bash
set -e

# Entrypoint script for running producer or consumer

COMPONENT="${1:-producer}"

case "$COMPONENT" in
    producer)
        echo "Starting Producer..."
        exec python main_producer.py
        ;;
    consumer)
        echo "Starting Consumer..."
        exec python main_consumer.py
        ;;
    *)
        echo "Usage: $0 [producer|consumer]"
        echo "Unknown component: $COMPONENT"
        exit 1
        ;;
esac


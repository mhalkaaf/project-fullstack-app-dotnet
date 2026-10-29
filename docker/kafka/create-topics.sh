#!/usr/bin/env bash
# This script runs inside the Linux Kafka container and must use LF line endings.
set -euo pipefail

topics=(
  orders.created
  inventory.reserved
  inventory.rejected
  payments.approved
  payments.rejected
  orders.completed
  orders.failed
  order-processing.dlq
)

for topic in "${topics[@]}"; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

#!/usr/bin/env bash
# Topics are created explicitly (auto-create is off) so partitions and
# retention are decisions, not accidents. Every topic is prefixed with the
# environment, so demo and production data never share a topic.
# Keyed by account_id: 6 partitions lets up to 6 scorer consumers share the
# load while keeping per-account order.
set -euo pipefail
ENVIRONMENT="${ENVIRONMENT:-demo}"
KT=/opt/kafka/bin/kafka-topics.sh
BS="${KAFKA_INTERNAL:-kafka:9092}"
create() { $KT --bootstrap-server "$BS" --create --if-not-exists --topic "$ENVIRONMENT.payments.$1.v1" \
             --partitions "$2" --replication-factor 1 --config retention.ms="$3"; }
create transactions            6 259200000    # raw events, 3 days (the replay source)
create transactions.validated  6 259200000    # forwarded by Spark for scoring
create labels                  3 604800000    # 7 days
create scored                  6 259200000    # stored scores and alert updates, feeds dashboards
create transactions.dlq        1 1209600000   # 14 days, for investigation and replay
$KT --bootstrap-server "$BS" --list

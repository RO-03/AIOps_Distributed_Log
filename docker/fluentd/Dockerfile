# docker/fluentd/Dockerfile
# Phase 2 — Fluentd Aggregator Container
# Tails the shared log file, parses raw BGL/synthetic logs,
# buffers on disk, and forwards to the Kafka cluster.

FROM fluent/fluentd:v1.16-1

USER root

# Install Kafka output plugin and retry support
RUN gem install \
    fluent-plugin-kafka:0.19.3 \
    fluent-plugin-record-transformer \
    --no-document

# Buffer directory for persistent chunk storage (mounted as Docker volume)
RUN mkdir -p /fluentd/buffer && chown -R fluent:fluent /fluentd/buffer

# Copy the configuration (will also be mounted via volume for live edits)
COPY fluent.conf /fluentd/etc/fluent.conf

USER fluent

EXPOSE 24224

CMD ["fluentd", "-c", "/fluentd/etc/fluent.conf", "-v"]

# docker/fluentd/Dockerfile
# Phase 2 — Fluentd Aggregator Container
# Tails the shared log file, parses raw BGL/synthetic logs,
# buffers on disk, and forwards to the Kafka cluster.

FROM fluent/fluentd:v1.16-1

USER root

# Install build dependencies for native extensions, install Kafka plugin, and clean up
RUN apk add --no-cache --update --virtual .build-deps \
        build-base ruby-dev \
    && gem install fluent-plugin-kafka:0.19.3 --no-document \
    && gem sources --clear-all \
    && apk del .build-deps \
    && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

# Buffer directory for persistent chunk storage (mounted as Docker volume)
RUN mkdir -p /fluentd/buffer && chown -R fluent:fluent /fluentd/buffer

# Copy the configuration (will also be mounted via volume for live edits)
COPY config/fluentd/fluent.conf /fluentd/etc/fluent.conf

USER fluent

EXPOSE 24224

CMD ["fluentd", "-c", "/fluentd/etc/fluent.conf", "-v"]

FROM rust:1.76-alpine as builder
RUN apk add --update --no-interactive --no-cache openssl-dev openssl-libs-static musl-dev && \
    rm -rf /var/cache/apk/*
WORKDIR /app
COPY . .
RUN cargo build --release \
    && mkdir -p /build \
    && cp /app/target/${CARGO_BUILD_TARGET}/release/sverige-news /build/ \
    && strip /build/sverige-news

FROM alpine:3.19
COPY --from=builder /build/ /
ENTRYPOINT ["/sverige-news"]
EXPOSE 8080
CMD ["--address", "0.0.0.0:8080", "--database-file", "/data/sverige-news.db"]

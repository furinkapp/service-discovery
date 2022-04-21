FROM rust:alpine as builder
WORKDIR /build
# copy source
COPY . .
# install musl-dev and build
RUN apk add --no-cache build-base musl-dev cmake && cargo build --release
# entrypoint layer
FROM alpine
WORKDIR /app
# copy binary and run
COPY --from=builder /build/target/release/service-discovery .
ENTRYPOINT [ "service-discovery" ]

# Build stage
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# VERSION is injected into the binary so the controller reports it to the
# Ingressive API via the User-Agent header. Set at build time via:
#   docker build --build-arg VERSION=0.1.0 .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w -X main.Version=${VERSION}" \
    -o /controller ./cmd/controller

# Final stage — minimal Alpine image with CA certs for TLS to the Bifrost API.
FROM alpine:latest

RUN apk --no-cache add ca-certificates
COPY --from=build /controller /controller

LABEL org.opencontainers.image.description="Ingressive Kubernetes Ingress Controller" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/ingressive-cloud/controller"

USER 65532:65532
ENTRYPOINT ["/controller"]

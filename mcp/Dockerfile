# minio-mcp — Contract A MCP sidecar for the shared MinIO object store.
# Multi-stage; tiny static binary on distroless. Pinned base digests belong in
# CI per the estate supply-chain rule (no floating :latest on money/infra path).
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /minio-mcp .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /minio-mcp /usr/local/bin/minio-mcp
USER nonroot:nonroot
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/minio-mcp"]

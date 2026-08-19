# minio-mcp — Contract A MCP sidecar for the shared MinIO object store.
# Multi-stage; tiny static binary on distroless. Base images are pinned by digest
# per the estate supply-chain rule (no floating tags on the money/infra path):
# `golang:1.22-alpine` and `distroless/static-debian12:nonroot` are moving tags,
# so a bare rebuild is not reproducible. Refresh a digest deliberately with
# `docker buildx imagetools inspect <ref>` and record it in the bump commit.
FROM golang:1.25-alpine@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /minio-mcp .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:1b7b9f0f0e0a1d2155f531db587cc48ec26aaf97ab64364225f5bf18a054e66a
COPY --from=build /minio-mcp /usr/local/bin/minio-mcp
USER nonroot:nonroot
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/minio-mcp"]

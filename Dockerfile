# syntax=docker/dockerfile:1

ARG GO_VERSION=1.24.3

# Install dbtestify command
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build-dbtestify
WORKDIR /src

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.mod,target=go.mod \
    go mod download -x
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 GOARCH=$TARGETARCH go build -ldflags="-s -w" -trimpath -o /bin/dbtestify ./cmd/dbtestify

# Deploy image
FROM gcr.io/distroless/base-debian12 AS final

COPY --from=build-dbtestify /bin/dbtestify /bin/dbtestify

EXPOSE 80

ENV DBTESTIFY_CONN=postgres://user:pass@postgres:5432/dbname?sslmode=disable

ENTRYPOINT [ "/bin/dbtestify" ]

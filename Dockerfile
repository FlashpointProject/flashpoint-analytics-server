FROM golang:1.27.4-trixie@sha256:f18c3d73a184fb0582828510722e723b63a93f7188d11dd223d9be6f6880e65d AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /analytics-intake ./main/*.go

FROM alpine:3.24.4@sha256:93b41ef40b1677fce8fcc64ff4af09daddfacf5040afde618bc46c1e2d1cc7d4
# The service opens GeoLite2-Country.mmdb relative to its working directory.
WORKDIR /app
COPY --from=build /analytics-intake /analytics-intake
COPY GeoLite2-Country.mmdb /app/GeoLite2-Country.mmdb
# Build with: docker build --build-arg GIT_COMMIT=$(git rev-parse HEAD) .
# (the service refuses to start without a non-empty GIT_COMMIT)
ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=$GIT_COMMIT
CMD ["/analytics-intake"]

FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/mockprovider ./cmd/mockprovider

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 10001 app
COPY --from=build /out/gateway /app/gateway
COPY --from=build /out/mockprovider /app/mockprovider
USER app
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["/app/gateway"]

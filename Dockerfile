FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/server ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache wget && adduser -D -u 10001 vaani
COPY --from=builder /out/server /server
EXPOSE 9091/tcp
USER vaani
ENTRYPOINT ["/server"]

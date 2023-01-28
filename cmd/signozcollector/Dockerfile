# Builder stage
FROM golang:1.18-alpine as build
RUN apk --update add ca-certificates

RUN export GOOS=$(echo ${TARGETPLATFORM} | cut -d / -f1) && \
    export GOARCH=$(echo ${TARGETPLATFORM} | cut -d / -f2) \

ARG TARGETPLATFORM

ENV OS111MODULE=on
ENV CGO_ENABLED=0

WORKDIR /src

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN cd cmd/signozcollector && go build -tags timetzdata -o /out/signoz-collector

# Final stage
FROM alpine:3.15

ARG USER_UID=10001

USER ${USER_UID}

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/signoz-collector /
COPY --from=build /src/exporter/clickhousetracesexporter/migrations /migrations
COPY --from=build /src/exporter/clickhouselogsexporter/migrations /logsmigrations

ENV LOG_MIGRATIONS_FOLDER="/logsmigrations"

EXPOSE 4317 4318
ENTRYPOINT ["/signoz-collector"]
CMD ["--config", "/etc/otel/config.yaml"]

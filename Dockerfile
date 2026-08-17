# syntax=docker/dockerfile:1

FROM docker.m.daocloud.io/library/golang:1.26.3-bookworm AS builder
WORKDIR /src
COPY . .
ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0
RUN go build -o /out/go-task-check .

FROM docker.m.daocloud.io/library/alpine:3.20
COPY --from=builder /out/go-task-check /usr/local/bin/go-task-check
ENTRYPOINT ["go-task-check"]
CMD ["--smoke-test"]

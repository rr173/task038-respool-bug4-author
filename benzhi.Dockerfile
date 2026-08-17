FROM golang:1.26.3

WORKDIR /app

COPY . .
RUN go build ./...

CMD ["bash"]

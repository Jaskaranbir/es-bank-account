# ===> Build Image
FROM golang:1.15.6-alpine3.12 AS builder
LABEL maintainer="Jaskaranbir Dhillon"

ENV CGO_ENABLED=0 \
    GOOS=linux

RUN apk add --update git

WORKDIR $GOPATH/src/github.com/Jaskaranbir/es-bank-account

COPY . ./
RUN go build -v -a -installsuffix nocgo -o /app ./main

# ===> Run Image
FROM scratch
LABEL maintainer="Jaskaranbir Dhillon"

COPY --from=builder /app ./
CMD ["./app"]

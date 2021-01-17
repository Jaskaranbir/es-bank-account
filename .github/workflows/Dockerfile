FROM golang:1.15.6-buster
LABEL maintainer="Jaskaranbir Dhillon"

LABEL "com.github.actions.icon"="code"
LABEL "com.github.actions.color"="green-dark"
LABEL "com.github.actions.name"="Go Tests"
LABEL "com.github.actions.description"="This action runs Go-tests."

ENV CGO_ENABLED=1 \
    GOOS=linux

WORKDIR $GOPATH/src/github.com/Jaskaranbir/es-bank-account

COPY ./ ./

CMD ["sh", ".github/workflows/entrypoint.sh"]

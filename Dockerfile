FROM golang:1.14.3

WORKDIR /src

COPY go.mod .
COPY go.sum .
RUN go mod download

ADD . .

CMD go test -count=1 -v -race ./...
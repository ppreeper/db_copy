default: all

build:
	go build -a ./...

install:
	go install -a ./...

all:
	GOOS=linux go install -ldflags="-s -w" -a ./...

default: all

build:
	go build -a ./...

install:
	go install -a ./...

all:
	GOOS=linux go build -ldflags="-s -w" -o dbcopy
	upx -9 dbcopy
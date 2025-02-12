default: build

tidy:
	go mod tidy -v

build:
	go vet ./...
	go build -v -race ./...

run-test:
	go test -v -run ^Test -race ./...
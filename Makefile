.PHONY: build api 
PKGS := $(shell go list ./... | grep -v idserver/api)
VERSION := $(shell git describe --always)
GOOS ?= linux
GOARCH ?= $(arch) 

build:
	@echo "Compiling source for $(GOOS) $(GOARCH)"
	@mkdir -p build
	@GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags "-X main.version=$(VERSION)" -o build/idserver_$(arch)$(BINEXT) cmd/idserver/main.go

clean:
	@echo "Cleaning up workspace"
	@rm -rf build

api:
	@echo "Generating API code from .proto files"
	@go generate api/ns/ns.go

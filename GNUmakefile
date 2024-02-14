OS            := $(shell go env GOOS)
ARCH          := $(shell go env GOARCH)
GO_FILES      := $(shell find . -type f -name '*.go')
HOSTNAME=registry.terraform.io
NAMESPACE=hashicorp
NAME=clickhouseops
VERSION=0.1.7
PLUGIN_PATH   ?= ${HOME}/.terraform.d/plugins/${HOSTNAME}/${NAMESPACE}/${NAME}/${VERSION}/${OS}_${ARCH}
PLUGIN_NAME   := terraform-provider-${NAME}
DIST_PATH     := dist/${OS}_${ARCH}

default: testacc

.PHONY: testacc
testacc:
	TF_ACC=1 go test ./... -v $(TESTARGS) -timeout 120m

${DIST_PATH}/${PLUGIN_NAME}: ${GO_FILES}
	mkdir -p $(DIST_PATH); \
	go build -o $(DIST_PATH)/${PLUGIN_NAME}

.PHONY: build
build: ${DIST_PATH}/${PLUGIN_NAME}

.PHONY: install
install: build
	mkdir -p $(PLUGIN_PATH); \
	rm -rf $(PLUGIN_PATH)/${PLUGIN_NAME}; \
	install -m 0755 $(DIST_PATH)/${PLUGIN_NAME} $(PLUGIN_PATH)/${PLUGIN_NAME}

.PHONY: clean
clean:
	rm -rf ${DIST_PATH}/*

APP_NAME := hkg-metatable
BUILD_VERSION   := $(shell git tag --contains)
BUILD_TIME      := $(shell date "+%F %T")
COMMIT_SHA1     := $(shell git rev-parse HEAD )

.PHONY: build
build:
	go build -ldflags \
		"\
		-X 'main.BuildVersion=${BUILD_VERSION}' \
		-X 'main.BuildTime=${BUILD_TIME}' \
		-X 'main.CommitID=${COMMIT_SHA1}' \
		"\
		-o ./bin/${APP_NAME}

.PHONY: run
run:
	./bin/${APP_NAME}

.PHONY: run-fs
run-fs:
	MSA_CONFIG_DEFINE='{"source":"file","prefix":"/etc/msa/","key":"metatable.yml"}' ./bin/${APP_NAME}

.PHONY: run-cs
run-cs:
	MSA_CONFIG_DEFINE='{"source":"consul","prefix":"/xtc/hkg/config","key":"metatable.yml"}' ./bin/${APP_NAME}

.PHONY: call
call:
	MICRO_REGISTRY=consul micro call xtc.api.hkg.metatable Healthy.Echo '{"msg":"hello"}'
	MICRO_REGISTRY=consul micro call xtc.api.hkg.metatable Vocabulary.ImportYaml '{"content":"labels:\n  - CPU\n  - PC\nvalues:\n  - i5\n  - i7\n  - i9"}'
	MICRO_REGISTRY=consul micro call xtc.api.hkg.metatable Vocabulary.List 
	MICRO_REGISTRY=consul micro call xtc.api.hkg.metatable Vocabulary.Find '{"name":"i5"}'

.PHONY: post
post:
	curl -X POST -d '{"msg":"hello"}' localhost:8080/hkg/metatable/Healthy/Echo

.PHONY: bm
bm:
	python3 benchmark.py

.PHONY: dist
dist:
	mkdir dist
	tar -zcf dist/${APP_NAME}-${BUILD_VERSION}.tar.gz ./bin/${APP_NAME}

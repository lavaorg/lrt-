APP = ekk

include $(MAKEPATH)/Makefile

NUM_INSTANCES=$(LOGGER_NUM_PARTITIONS)

compile:
	docker run --rm -v $(ROOT):$(ROOT) -w $(BASE_COMPILE_DIR) \
		-e GOPATH=$(GOPATH) $(BUILDER) go build -o $(APP) cmd/ekk/main.go

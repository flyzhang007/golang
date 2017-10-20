TARGET=EBS
SRCS=$(EBSROOT)/src/main.go

GO=go
GCOPTS=-gcflags "-N -l"

BIN_DIR=$(EBSROOT)/bin

.PHONY: make clean
make:
	$(GO) build -i -o $(BIN_DIR)/$(TARGET) $(GCOPTS) $(SRCS)

clean:
	rm -f $(BIN_DIR)/$(TARGET)

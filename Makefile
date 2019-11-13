
test: dep
	@GO111MODULE=on go test -cover ./...

.PHONY: dep test

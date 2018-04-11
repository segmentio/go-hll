
dep:
	@dep ensure

test: dep
	@go test -cover ./...

.PHONY: dep test

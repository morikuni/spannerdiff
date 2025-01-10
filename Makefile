.PHONY: test
test:
	@go test -v -count 1 ./...

.PHONY: coverage
coverage:
	@go test -v -count 1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: test
test:
	@go test -v -count 1 ./...

.PHONY: coverage
coverage:
	@go test -v -count 1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: lint
lint:
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

EMULATOR_IMAGE := gcr.io/cloud-spanner-emulator/emulator:1.5.58

.PHONY: test
test:
	@go test -v -count 1 ./...

# test-emulator also verifies the generated DDLs on the Spanner emulator.
.PHONY: test-emulator
test-emulator:
	@docker run -d --rm --name spannerdiff-emulator -p 9020:9020 $(EMULATOR_IMAGE) > /dev/null
	@SPANNER_EMULATOR_REST_HOST=localhost:9020 go test -count 1 ./...; status=$$?; docker rm -f spannerdiff-emulator > /dev/null; exit $$status

.PHONY: coverage
coverage:
	@go test -v -count 1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: lint
lint:
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

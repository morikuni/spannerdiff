OMNI_IMAGE := us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts

.PHONY: test
test:
	@go test -v -count 1 ./...

# test-omni also verifies the generated DDLs on Spanner Omni. It takes several minutes.
.PHONY: test-omni
test-omni:
	@docker run -d --rm --name spannerdiff-omni $(OMNI_IMAGE) start-single-server > /dev/null
	@until docker logs spannerdiff-omni 2>&1 | grep -q "Spanner is ready"; do sleep 1; done
	@SPANNER_OMNI_CONTAINER=spannerdiff-omni go test -count 1 -parallel 64 -timeout 30m ./...; status=$$?; docker rm -f spannerdiff-omni > /dev/null; exit $$status

.PHONY: coverage
coverage:
	@go test -v -count 1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: lint
lint:
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

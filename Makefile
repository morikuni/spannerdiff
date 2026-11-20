EMULATOR_IMAGE := gcr.io/cloud-spanner-emulator/emulator:1.5.58
OMNI_IMAGE := us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts

.PHONY: test
test:
	@go test -v -count 1 ./...

# test-emulator also verifies the generated DDLs on the Spanner emulator, as CI does.
.PHONY: test-emulator
test-emulator:
	@docker run -d --rm --name spannerdiff-emulator -p 9020:9020 $(EMULATOR_IMAGE) > /dev/null
	@SPANNER_EMULATOR_REST_HOST=localhost:9020 go test -count 1 ./...; status=$$?; docker rm -f spannerdiff-emulator > /dev/null; exit $$status

# test-omni also verifies the generated DDLs on Spanner Omni, which is Spanner itself. It takes several minutes.
# CI doesn't run it, because schema changes that add stored generated columns often don't finish on GitHub Actions.
.PHONY: test-omni
test-omni:
	@docker run -d --rm --name spannerdiff-omni $(OMNI_IMAGE) start-single-server > /dev/null
	@for i in $$(seq 60); do docker logs spannerdiff-omni 2>&1 | grep -q "Spanner is ready" && break; sleep 1; done
	@SPANNER_OMNI_CONTAINER=spannerdiff-omni go test -count 1 -parallel 64 -timeout 15m ./...; status=$$?; docker rm -f spannerdiff-omni > /dev/null; exit $$status

.PHONY: coverage
coverage:
	@go test -v -count 1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out

.PHONY: lint
lint:
	@go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run

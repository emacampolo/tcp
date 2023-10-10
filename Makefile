.PHONY: all
all: tidy format vet test

.PHONY: tidy
tidy:
	@echo "=> Executing go mod tidy"
	@go mod tidy

.PHONY: format
format:
	@echo "=> Formatting code and organizing imports"
	@goimports -w ./

.PHONY: vet
vet:
	@echo "=> Executing go vet"
	@go vet ./...

COMMON_FLAGS := -covermode=atomic -coverprofile=/tmp/coverage.out -coverpkg=./... -count=1 -race -shuffle=on

.PHONY: test
test:
	@go test ./... $(COMMON_FLAGS)
	@cat .covignore | sed '/^[[:space:]]*$$/d' >/tmp/covignore
	@grep -Fvf /tmp/covignore /tmp/coverage.out  > /tmp/coverage.out-filtered
	@go tool cover -func /tmp/coverage.out-filtered | grep total: | sed -e 's/\t//g' | sed -e 's/(statements)/ /'

.PHONY: test-cover
test-cover: test
	@echo "=> Running tests and generating report"
	@go tool cover -html=/tmp/coverage.out-filtered

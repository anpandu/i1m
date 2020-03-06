fmt:
	go fmt ./...
test:
	go test ./internal/...
test-v:
	go test ./internal/... -v
test-bench:
	go test ./internal/...  -bench='.'
test-all:
	go test ./internal/... -v -bench='.'
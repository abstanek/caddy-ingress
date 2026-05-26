build:
	@mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./bin/ingress-controller ./cmd/caddy

dev:
	skaffold dev --port-forward

.PHONY: e2e
e2e:
	python3 test/e2e/e2e.py $(E2E_ARGS)

# Repository Guidelines

## Project Structure & Module Organization

This is a Go Prometheus exporter for VMware NSX Advanced Load Balancer. The
entry point is `main.go`; core packages are organized by responsibility:

- `avi/`: Avi Controller API client, response types, inventory, and analytics helpers.
- `collector/`: Prometheus collectors, cache refresh logic, topology modeling, and metric modules.
- `config/`: environment variable and flag parsing helpers.
- `docs/`: research and reference notes.
- `scripts/`: release automation helpers used by GitHub Actions.

Tests live beside the code as `*_test.go`.

## Build, Test, and Development Commands

- `go test ./... -cover`: run the full test suite with statement coverage.
- `go test -race ./...`: run race-detector tests.
- `go vet ./...`: run static checks used by CI.
- `go build -o avi-exporter .`: build a local binary.
- `go run . -url https://avi.example.com -tenants '*'`: run locally against a controller; credentials come from `AVI_USERNAME` and `AVI_PASSWORD`.
- `npm ci --ignore-scripts`: install semantic-release tooling.

The `Dockerfile` expects GoReleaser-style platform outputs, so use GoReleaser
for release images.

## Coding Style & Naming Conventions

Run `gofmt` on changed Go files. Keep package names short and lowercase (`avi`,
`collector`, `config`). Exported identifiers should have useful Go doc comments
when they cross a package boundary. Prefer table-driven tests.

Configuration names should follow existing `AVI_*` environment variables and
kebab-case CLI flags, for example `AVI_DISABLED_MODULES` and `-disabled-modules`.

## Testing Guidelines

The README states 100% statement coverage across packages; preserve that for
behavior changes. Add focused `TestXxx` tests next to the package under change.
For collector changes, cover emitted values and edge cases such as disabled
modules, empty responses, and refresh failures.

Before opening a PR, run:

```bash
go test ./... -cover
go vet ./...
go test -race ./...
```

## Commit & Pull Request Guidelines

Releases are driven by Conventional Commits via semantic-release. Use messages
such as `fix: handle empty pool runtime`, `feat: add gslb metrics`, or `chore:
update release tooling`. `fix` creates patch releases; `feat` creates minor
releases.

Pull requests should describe user-visible changes, list validation commands,
and link related issues when available. Include sample metric names or CLI
examples when changing exporter output or configuration.

## Security & Configuration Tips

Do not commit controller URLs, usernames, passwords, API tokens, or CA bundles.
Use environment variables for local runs. Prefer `AVI_CA_FILE` over
`AVI_IGNORE_CERT` when testing against private CAs.

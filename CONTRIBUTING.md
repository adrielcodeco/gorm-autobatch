# Contributing

## Setup

```bash
git clone https://github.com/adrielcodeco/gorm-autobatch
cd gorm-autobatch
go mod download
```

## Running tests

```bash
# Unit + integration tests with race detector
go test -race ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Pull requests

- Keep changes focused — one concern per PR
- All tests must pass with `-race`
- Coverage should not decrease
- No comments explaining what the code does, only why when non-obvious

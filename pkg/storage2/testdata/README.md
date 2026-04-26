# Storage2 test fixtures

Fixtures for format and behavior comparison with Lance semantics.

- **minimal.manifest**: Minimal manifest (version 0, one fragment, one data file). Proto binary.
- **sample_append.txn**: Single Append transaction (read_version 0, one fragment). Proto binary.

Generate or update fixtures by running from the package directory:

	go test -run TestGenerateFixtures -v .

Or from repo root:

	cd pkg/storage2 && go test -run TestGenerateFixtures -v .

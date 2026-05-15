.PHONY: help
help: ## Show this help
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_.-]+:.*##' $(MAKEFILE_LIST) | while IFS= read -r line; do \
		target=$$(echo "$$line" | cut -d: -f1); \
		desc=$$(echo "$$line" | sed 's/.*## *//'); \
		printf "  \033[36m%-24s\033[0m %s\n" "$$target" "$$desc"; \
	done | sort
	@echo ""
	@echo "Examples:"
	@echo "  make test-tpch-init                   Init TPC-H: create tables + load data"
	@echo "  make test-tpch-bench                  Full TPC-H benchmark: init + run all"
	@echo "  make test-tpch-q Q=4                  Run TPC-H query 4 only"
	@echo "  make test-tpch TPCH_DB=/path/to/db    Override DB path"
	@echo "  make test-tpcds-q Q=1                 Run TPC-DS query 1 only"
	@echo ""

.PHONY: plandb
plandb: ## Build plandb binary
	@go build -o plandb cmd/main/main.go

.PHONY: tester
tester: ## Build tester binary
	@go build -o tester cmd/tester/main.go

.PHONY: config
config: ## Run go mod tidy
	$(info [go tidy])
	@go mod tidy
.PHONY: clean-data
clean-data: ## Remove all database files (TPCH + TPCDS)
	rm -f tpch.db tpch.db.wal tpcds.db tpcds.db.wal

############# Code Quality

.PHONY: fmt
fmt: ## Format all Go source files
	gofmt -l -s -w .

############# TPC-H Tests

TPCH_DB ?= $(CURDIR)/tpch.db
TPCH_CONFIG ?= $(CURDIR)/etc/tpch/1g/tester.toml
TPCH_DATA_DIR ?= /home/pengzhen/Documents/tpch-parquet

.PHONY: test-tpch
test-tpch: tester ## Run all 22 TPC-H queries
	@TESTER_CONFIG_PATH=$(TPCH_CONFIG) DB_PATH=$(TPCH_DB) ./tester tpch1g --query_id 0

.PHONY: test-tpch-q
test-tpch-q: tester ## Run single TPC-H query (make test-tpch-q Q=4)
	@if [ -n "$(Q)" ]; then TESTER_CONFIG_PATH=$(TPCH_CONFIG) DB_PATH=$(TPCH_DB) ./tester tpch1g --query_id $(Q); else echo "Usage: make test-tpch-q Q=<id>"; fi

.PHONY: test-tpch-load
test-tpch-load: tester ## Load TPC-H data from parquet files (TPCH_DATA_DIR)
	@echo "Loading TPC-H data from $(TPCH_DATA_DIR)..."
	@DB_PATH=$(TPCH_DB) ./tester tpch1gddl --ddl "copy nation   from '$(TPCH_DATA_DIR)/nation.parquet'   with (FORMAT 'parquet'); \
	copy region   from '$(TPCH_DATA_DIR)/region.parquet'   with (FORMAT 'parquet'); \
	copy part     from '$(TPCH_DATA_DIR)/part.parquet'     with (FORMAT 'parquet'); \
	copy supplier from '$(TPCH_DATA_DIR)/supplier.parquet' with (FORMAT 'parquet'); \
	copy partsupp from '$(TPCH_DATA_DIR)/partsupp.parquet' with (FORMAT 'parquet'); \
	copy customer from '$(TPCH_DATA_DIR)/customer.parquet' with (FORMAT 'parquet'); \
	copy orders   from '$(TPCH_DATA_DIR)/orders.parquet'   with (FORMAT 'parquet'); \
	copy lineitem from '$(TPCH_DATA_DIR)/lineitem.parquet' with (FORMAT 'parquet');"

.PHONY: test-tpch-init
test-tpch-init: clean-data tester ## Init TPC-H: drop old DB + create tables + load data
	@echo "Creating TPC-H database at $(TPCH_DB)..."
	@DB_PATH=$(TPCH_DB) ./tester tpch1gddl --path $(CURDIR)/cases/tpch/query/ddl.sql
	@$(MAKE) test-tpch-load TPCH_DB=$(TPCH_DB) TPCH_DATA_DIR=$(TPCH_DATA_DIR)

.PHONY: test-tpch-bench
test-tpch-bench: test-tpch-init ## Full TPC-H benchmark: init + run all 22 queries
	@$(MAKE) test-tpch TPCH_DB=$(TPCH_DB)

.PHONY: test-tpch-ddl
test-tpch-ddl: tester ## Create TPC-H tables (DDL only, no data)
	@echo "Creating TPC-H tables..."
	@DB_PATH=$(TPCH_DB) ./tester tpch1gddl --path $(CURDIR)/cases/tpch/query/ddl.sql

############# TPC-DS Tests

TPCDS_DB ?= $(CURDIR)/tpcds.db
TPCDS_CONFIG ?= $(CURDIR)/etc/tpcds/tester.toml

.PHONY: test-tpcds
test-tpcds: tester ## Run all 99 TPC-DS queries
	@TESTER_CONFIG_PATH=$(TPCDS_CONFIG) DB_PATH=$(TPCDS_DB) ./tester tpch1g --need_headline false

.PHONY: test-tpcds-q
test-tpcds-q: tester ## Run single TPC-DS query (make test-tpcds-q Q=1)
	@if [ -n "$(Q)" ]; then TESTER_CONFIG_PATH=$(TPCDS_CONFIG) DB_PATH=$(TPCDS_DB) ./tester tpch1g --query_id $(Q) --need_headline false; else echo "Usage: make test-tpcds-q Q=<id>"; fi

.PHONY: test-tpcds-ddl
test-tpcds-ddl: tester ## Load TPC-DS DDL into database
	@echo "Loading TPC-DS DDL..."
	@TESTER_CONFIG_PATH=$(TPCDS_CONFIG) DB_PATH=$(TPCDS_DB) ./tester tpch1gddl --path $(CURDIR)/cases/tpcds/tpcds.sql

############# Code Quality

.PHONY: install-static-check-tools
install-static-check-tools: ## Install linting tools
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(GOPATH)/bin v1.61.0
	@go install github.com/matrixorigin/linter/cmd/molint@latest
	@go install github.com/apache/skywalking-eyes/cmd/license-eye@v0.4.0

.PHONY: static-check
static-check: config ## Run static analysis
	#$(CGO_OPTS) go vet -vettool=`which molint` ./...
	$(CGO_OPTS) license-eye -c .licenserc.yml header check
	$(CGO_OPTS) license-eye -c .licenserc.yml dep check
	$(CGO_OPTS) golangci-lint run -c .golangci.yml ./...


fmtErrs := $(shell grep -onr 'fmt.Errorf' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)
errNews := $(shell grep -onr 'errors.New' pkg/ --exclude-dir=.git --exclude-dir=vendor \
				--exclude=*.pb.go --exclude=system_vars.go --exclude=Makefile)

.PHONY: err-check
err-check:
#ifneq ("$(strip $(fmtErrs))$(strip $(errNews))", "")
# ifneq ("$(strip $(fmtErrs))", "")
#		$(warning 'fmt.Errorf()' is found.)
#		$(warning One of 'fmt.Errorf()' is called at: $(shell printf "%s\n" $(fmtErrs) | head -1))
# endif
# ifneq ("$(strip $(errNews))", "")
#		$(warning 'errors.New()' is found.)
#		$(warning One of 'errors.New()' is called at: $(shell printf "%s\n" $(errNews) | head -1))
# endif
#
#	$(error Use xxxerr instead.)
#else
#	$(info Neither 'fmt.Errorf()' nor 'errors.New()' is found)
#endif

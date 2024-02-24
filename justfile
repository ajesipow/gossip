#!/usr/bin/env just --justfile
# Formats and checks the code
all: format check
ci: _add-target format check

[private]
alias a := all
[private]
alias c := check
[private]
alias f := format

# Adds the target for CI
_add-target:
	rustup target add aarch64-unknown-linux-gnu

# Run clippy and formatter
check: _c-clippy _c-fmt

_c-clippy:
	cargo clippy -j4 --all-targets -- -D warnings

_c-fmt: update-nightly-fmt
	cargo +nightly-2023-12-07 fmt --all -- --check

_c-fix:
    cargo fix --workspace

# Format the code
format: update-nightly-fmt
	cargo +nightly-2023-12-07 fmt --all

clean:
  cargo clean

# Fix clippy warnings and format the code.
fix: _c-fix format

# Installs/updates the nightly rustfmt installation
update-nightly-fmt:
	rustup toolchain install --profile minimal nightly-2023-12-07 --no-self-update
	rustup component add rustfmt --toolchain nightly-2023-12-07

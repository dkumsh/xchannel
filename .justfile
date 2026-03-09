# print options
default:
    @just --list --unsorted

# install cargo tools
init:
    cargo upgrade --incompatible
    cargo update

# check code
check:
    cargo check
    cargo fmt --all -- --check
    cargo clippy --all-targets --all-features

# automatically fix clippy warnings
fix:
    cargo fmt --all
    cargo clippy --allow-dirty --allow-staged --fix

# build project
build:
   cargo build --all-targets

# execute tests
test:
   cargo test

# execute benchmarks
bench:
    cargo bench

# stress rolling for two-readers example
stress:
    cargo run --example two_readers --release -- --file xchan --messages 5m --region-size 1m --roll-size 8m --burst-pause-us 1 --burst-size 5

# bursty workload with batch coalescing
burst:
    # 16k region + 32k roll => ~341 msgs/region and ~682 msgs/file, so a 1024 batch spans 3+ regions and 2+ files.
    # burst pause gives readers time to drain each burst; total runtime ~10-30s.
    cargo run --example two_readers --release -- --file xchan --messages 1m --batch-size 1024 --burst-size 10000 --burst-pause-us 200000 --region-size 16k --roll-size 32k --key-space 256 --work-us 10

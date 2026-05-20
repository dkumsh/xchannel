set export := true

# Knobs for `bench` / `bench-quick`. Override on the command line, e.g.:
#   just WRITER_CORE=10 READER_CORE=11 bench
#   just SIZES="64 1k" DURATION=10 bench
WRITER_CORE := "4"
READER_CORE := "3"
DURATION    := "30"
WARMUP      := "3"
SIZES       := "64 256 4k"
KEEP_FILES  := "2"
DISK_PATH   := "./bench/.disk"
TMPFS_PATH  := "/dev/shm"
OUT_DIR     := "./bench"

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

# run the full latency benchmark matrix; writes bench/results-<hostname>.md
bench:
    @bench/run.sh

# quick smoke (one msg size, 5s window) — for verifying setup
bench-quick:
    @bench/run.sh --quick

# build the bench binary in release mode without running it
bench-build:
    @cargo build --release --example xch-bench

# stress rolling for two-readers example
stress:
    cargo run --example two_readers --release -- --file xchan --messages 5m --region-size 1m --roll-size 8m --burst-pause-us 1 --burst-size 5

# bursty workload with batch coalescing
burst:
    # 16k region + 32k roll => ~341 msgs/region and ~682 msgs/file, so a 1024 batch spans 3+ regions and 2+ files.
    # burst pause gives readers time to drain each burst; total runtime ~10-30s.
    cargo run --example two_readers --release -- --file xchan --messages 1m --batch-size 1024 --burst-size 10000 --burst-pause-us 200000 --region-size 16k --roll-size 32k --key-space 256 --work-us 10

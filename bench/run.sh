#!/usr/bin/env bash
# bench/run.sh — drive the xchannel latency benchmark matrix.
#
# Spawns one (writer, reader) pair per cell, each pinned to a dedicated CPU
# core via the binary's --affinity flag (sched_setaffinity). The reader prints
# one JSON summary line per cell on stdout; this script collects those lines
# and emits a Markdown report with a system-info preamble.
#
# Usage: bench/run.sh [--quick]
#
# Environment overrides (all optional):
#   WRITER_CORE   CPU core for the writer process       (default 4)
#   READER_CORE   CPU core for the reader process       (default 3)
#   DURATION      measurement window per cell, seconds  (default 30)
#   WARMUP        warmup window per cell, seconds       (default 3)
#   DISK_PATH     directory for the persistent backend  (default ./bench/.disk)
#   TMPFS_PATH    directory for the tmpfs backend       (default /dev/shm)
#   OUT_DIR       output directory for results          (default ./bench)
#   SIZES         space-separated msg sizes             (default "64 256 4k")
#   ROLL_SIZE     file roll size                        (default 1g)
#   REGION_SIZE   region size                           (default 16m)
#   KEEP_FILES    files retained on disk (0=unlimited)  (default 2)
#   GAPS_NS       space-separated publish gaps in ns    (default "0 1000 100000")
#                 0 = unthrottled (writer saturation)
#   HOST_LABEL    label for the report (title + filename) (default $(hostname -s))
#
# File rolling is enabled by default to keep memory bounded — without it the
# tmpfs backend allocates GBs of RAM during a saturated 30s run and the
# numbers reflect memory pressure, not channel latency.
#
# Quick mode (--quick): one msg size, 5s window — ~10s total. For smoke tests.

set -euo pipefail

WRITER_CORE="${WRITER_CORE:-4}"
READER_CORE="${READER_CORE:-3}"
DURATION="${DURATION:-30}"
WARMUP="${WARMUP:-3}"
DISK_PATH="${DISK_PATH:-./bench/.disk}"
TMPFS_PATH="${TMPFS_PATH:-/dev/shm}"
OUT_DIR="${OUT_DIR:-./bench}"
SIZES="${SIZES:-64 256 4k}"
ROLL_SIZE="${ROLL_SIZE:-1g}"
REGION_SIZE="${REGION_SIZE:-16m}"
KEEP_FILES="${KEEP_FILES:-2}"
GAPS_NS="${GAPS_NS:-0 1000 100000}"

if [[ "${1:-}" == "--quick" ]]; then
  SIZES="64"
  GAPS_NS="0 1000"
  DURATION=5
  WARMUP=2
fi

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

bin="${BENCH_BIN:-$repo_root/target/release/examples/xchan_bench}"
if [[ ! -x "$bin" ]]; then
  echo "[run] building xchan_bench (release)..." >&2
  cargo build --release --example xchan_bench >&2
  bin="$repo_root/target/release/examples/xchan_bench"
fi

mkdir -p "$DISK_PATH" "$OUT_DIR"
host="${HOST_LABEL:-$(hostname -s)}"
out="$OUT_DIR/results-$host.md"

# ---- system info preamble ----
{
  echo "# xchannel benchmark — \`$host\`"
  echo
  echo "_Generated $(date -Is) by \`bench/run.sh\`._"
  echo
  echo "## System"
  echo
  echo '```'
  echo "host:        $host"
  echo "kernel:      $(uname -srvmo)"
  if [[ -r /etc/os-release ]]; then
    . /etc/os-release
    echo "os:          ${PRETTY_NAME:-unknown}"
  fi
  cpu_model=$(awk -F: '/^model name/ {print $2; exit}' /proc/cpuinfo | sed 's/^ *//')
  echo "cpu:         $cpu_model"
  echo "cores:       $(nproc) logical"
  if command -v lscpu >/dev/null 2>&1; then
    numa=$(lscpu | awk -F: '/^NUMA node\(s\)/ {print $2}' | tr -d ' ')
    echo "numa nodes:  ${numa:-?}"
  fi
  echo "page size:   $(getconf PAGESIZE) bytes"
  echo "rustc:       $(rustc --version 2>/dev/null || echo unknown)"
  if [[ -r /proc/cmdline ]]; then
    echo "cmdline:     $(cat /proc/cmdline)"
  fi
  echo
  echo "writer core: $WRITER_CORE"
  echo "reader core: $READER_CORE"
  echo "duration:    ${DURATION}s + ${WARMUP}s warmup per cell"
  echo "region size: $REGION_SIZE"
  echo "roll size:   $ROLL_SIZE"
  echo "keep files:  $KEEP_FILES"
  echo "disk path:   $DISK_PATH ($(stat -f -c %T "$DISK_PATH" 2>/dev/null || echo ?))"
  echo "tmpfs path:  $TMPFS_PATH ($(stat -f -c %T "$TMPFS_PATH" 2>/dev/null || echo ?))"
  echo '```'
  echo
  echo "## Methodology"
  echo
  echo "- Writer process pinned to core \`$WRITER_CORE\`, reader pinned to core \`$READER_CORE\`."
  echo "- Each cell: ${WARMUP}s warmup + ${DURATION}s measurement."
  echo "- Publish gaps tested per size: \`$GAPS_NS\` ns. \`gap=0\` is unthrottled (writer saturation);"
  echo "  non-zero gaps busy-wait between publishes via \`--gap-ns N\` to simulate realistic load."
  echo "- File rolling enabled (\`--roll-size $ROLL_SIZE\`, \`--region-size $REGION_SIZE\`),"
  echo "  retention capped at \`--keep-files $KEEP_FILES\` so the writer prunes old rolled files."
  echo "- Latency is end-to-end: writer stamps a monotonic ns timestamp into the payload;"
  echo "  reader subtracts \`now - sent_ts\` on receipt."
  echo "- Reader touches the entire payload (XOR-fold) to defeat dead-store elimination."
  echo "- Histograms via HdrHistogram (3 sig figs, 1 ns – 60 s)."
  echo "- All numbers below are reader-side end-to-end, post-warmup."
  echo
  echo "## Results"
  echo
} > "$out"

# ---- helpers ----
run_cell() {
  local backend="$1" base="$2" size="$3" gap="$4"
  local tag="$backend-$size-g$gap"
  local file="$base/xc_bench_$$_${backend}_${size}_${gap}"
  rm -f "${file}" "${file}".*

  # Writer first so the file/region exists when the reader opens it.
  "$bin" --writer -f "$file" -s "$size" \
    --duration-secs $((DURATION + WARMUP + 2)) \
    --affinity "$WRITER_CORE" \
    --region-size "$REGION_SIZE" \
    --roll-size "$ROLL_SIZE" \
    --keep-files "$KEEP_FILES" \
    --gap-ns "$gap" \
    >/dev/null 2>"$OUT_DIR/.writer-$tag.err" &
  local wpid=$!

  # Give the writer a moment to create the file.
  sleep 0.3

  local json
  if ! json=$("$bin" --reader -f "$file" -s "$size" \
        --duration-secs "$DURATION" --warmup-secs "$WARMUP" \
        --affinity "$READER_CORE" \
        2>"$OUT_DIR/.reader-$tag.err"); then
    echo "[run] reader failed for $tag" >&2
    cat "$OUT_DIR/.reader-$tag.err" >&2
    kill "$wpid" 2>/dev/null || true
    wait "$wpid" 2>/dev/null || true
    rm -f "${file}" "${file}".*
    return 1
  fi

  # Tear down writer.
  kill "$wpid" 2>/dev/null || true
  wait "$wpid" 2>/dev/null || true
  # Rolling produces base, base.1, base.2, ... — clean them all.
  rm -f "${file}" "${file}".*

  printf '%s' "$json"
}

fmt_gap() {
  # Pretty-print a publish-gap (ns) for the `gap` table column.
  awk -v n="$1" 'BEGIN{
    if (n+0 == 0) { print "sat"; exit }
    if (n < 1000) { printf "%d ns", n }
    else if (n < 1000000) { printf "%.0f µs", n/1000 }
    else { printf "%.0f ms", n/1000000 }
  }'
}

# Extract a numeric JSON field. (Avoids a jq dependency.)
jget() {
  local key="$1" json="$2"
  echo "$json" | sed -n "s/.*\"$key\":\\([^,}]*\\).*/\\1/p"
}

fmt_ns() {
  # Pretty-print a ns value as ns / µs / ms.
  awk -v n="$1" 'BEGIN{
    if (n+0 == 0) { print "—"; exit }
    if (n < 1000) { printf "%d ns", n }
    else if (n < 1000000) { printf "%.2f µs", n/1000 }
    else if (n < 1000000000) { printf "%.2f ms", n/1000000 }
    else { printf "%.2f s", n/1000000000 }
  }'
}

fmt_rate() {
  awk -v r="$1" 'BEGIN{
    if (r+0 == 0) { print "—"; exit }
    if (r >= 1e6) printf "%.2f M/s", r/1e6
    else if (r >= 1e3) printf "%.2f K/s", r/1e3
    else printf "%.0f /s", r
  }'
}

emit_table() {
  local backend="$1" path="$2"
  echo "### Backend: \`$backend\` (\`$path\`)" >> "$out"
  echo >> "$out"

  for gap in $GAPS_NS; do
    {
      echo "**Publish gap: \`$(fmt_gap "$gap")\`**"
      echo
      echo "| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |"
      echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    } >> "$out"

    for size in $SIZES; do
      echo "[run] $backend gap_ns=$gap size=$size ..." >&2
      local json
      json=$(run_cell "$backend" "$path" "$size" "$gap") || continue
      echo "[run] $backend gap_ns=$gap size=$size -> $json" >&2

      local samples rate p50 p90 p95 p99 p999 mx
      samples=$(jget samples "$json")
      rate=$(jget msgs_per_sec "$json")
      p50=$(jget p50_ns "$json")
      p90=$(jget p90_ns "$json")
      p95=$(jget p95_ns "$json")
      p99=$(jget p99_ns "$json")
      p999=$(jget p999_ns "$json")
      mx=$(jget max_ns "$json")

      printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
        "$size" \
        "$(fmt_rate "$rate")" \
        "$(fmt_ns "$p50")" \
        "$(fmt_ns "$p90")" \
        "$(fmt_ns "$p95")" \
        "$(fmt_ns "$p99")" \
        "$(fmt_ns "$p999")" \
        "$(fmt_ns "$mx")" \
        "$samples" \
        >> "$out"
    done

    echo >> "$out"
  done
}

emit_table tmpfs "$TMPFS_PATH"
emit_table disk "$DISK_PATH"

# Strip per-cell stderr scratch files.
rm -f "$OUT_DIR"/.reader-*.err "$OUT_DIR"/.writer-*.err

echo "[run] wrote $out" >&2

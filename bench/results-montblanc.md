# xchannel benchmark — `montblanc`

_Generated 2026-04-30T14:32:26+01:00 by `bench/run.sh`._

## System

```
host:        montblanc
kernel:      Linux 6.17.0-22-generic #22-Ubuntu SMP PREEMPT_DYNAMIC Fri Mar 13 12:04:44 UTC 2026 x86_64 GNU/Linux
os:          Ubuntu 25.10
cpu:         11th Gen Intel(R) Core(TM) i9-11900H @ 2.50GHz
cores:       16 logical
numa nodes:  1
page size:   4096 bytes
rustc:       rustc 1.94.0 (4a4ef493e 2026-03-02)
cmdline:     BOOT_IMAGE=/boot/vmlinuz-6.17.0-22-generic root=UUID=b5dbca85-964c-405b-bee5-766a4d57afed ro ipv6.disable=1 quiet splash crashkernel=2G-4G:320M,4G-32G:512M,32G-64G:1024M,64G-128G:2048M,128G-:4096M vt.handoff=7

writer core: 4
reader core: 3
duration:    30s + 3s warmup per cell
region size: 16m
roll size:   1g
keep files:  2
disk path:   ./bench/.disk (ext2/ext3)
tmpfs path:  /dev/shm (tmpfs)
```

## Methodology

- Writer process pinned to core `4`, reader pinned to core `3`.
- Each cell: 3s warmup + 30s measurement.
- Publish gaps tested per size: `0 1000 100000` ns. `gap=0` is unthrottled (writer saturation);
  non-zero gaps busy-wait between publishes via `--gap-ns N` to simulate realistic load.
- File rolling enabled (`--roll-size 1g`, `--region-size 16m`),
  retention capped at `--keep-files 2` so the writer prunes old rolled files.
- Latency is end-to-end: writer stamps a monotonic ns timestamp into the payload;
  reader subtracts `now - sent_ts` on receipt.
- Reader touches the entire payload (XOR-fold) to defeat dead-store elimination.
- Histograms via HdrHistogram (3 sig figs, 1 ns – 60 s).
- All numbers below are reader-side end-to-end, post-warmup.

## Results

### Backend: `tmpfs` (`/dev/shm`)

**Publish gap: `sat`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 9.40 M/s | 86 ns | 248.70 µs | 414.21 µs | 951.81 µs | 1.77 ms | 80.15 ms | 281955262 |
| 256 | 5.13 M/s | 160 ns | 308.74 µs | 398.85 µs | 699.90 µs | 1.11 ms | 108.99 ms | 153994983 |
| 4k | 479.04 K/s | 828 ns | 392.45 µs | 486.40 µs | 815.62 µs | 1.51 ms | 89.46 ms | 14371180 |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1.00 M/s | 52 ns | 87 ns | 298 ns | 405.50 µs | 793.60 µs | 70.06 ms | 30000000 |
| 256 | 1.00 M/s | 57 ns | 3.68 µs | 325.89 µs | 485.63 µs | 813.57 µs | 88.21 ms | 30000000 |
| 4k | 474.46 K/s | 773 ns | 401.15 µs | 517.12 µs | 781.82 µs | 1.14 ms | 93.78 ms | 14233940 |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 10.00 K/s | 54 ns | 93 ns | 242 ns | 2.85 µs | 8.13 µs | 5.59 ms | 300000 |
| 256 | 10.00 K/s | 55 ns | 208 ns | 291 ns | 2.19 µs | 251.78 µs | 5.62 ms | 300000 |
| 4k | 10.00 K/s | 446 ns | 738 ns | 2.47 µs | 469.25 µs | 869.89 µs | 10.12 ms | 300000 |

### Backend: `disk` (`./bench/.disk`)

**Publish gap: `sat`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 8.60 M/s | 75 ns | 124 ns | 232 ns | 36.45 µs | 233.09 µs | 65.70 ms | 258061723 |
| 256 | 4.52 M/s | 131 ns | 403 ns | 1.06 µs | 74.24 µs | 309.76 µs | 60.10 ms | 135454881 |
| 4k | 384.29 K/s | 1.47 µs | 2.04 µs | 8.00 µs | 73.73 µs | 175.74 µs | 61.80 ms | 11528640 |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1.00 M/s | 51 ns | 80 ns | 178 ns | 5.91 µs | 92.67 µs | 44.70 ms | 30000000 |
| 256 | 1.00 M/s | 57 ns | 470 ns | 1.06 µs | 35.13 µs | 155.39 µs | 44.89 ms | 30000000 |
| 4k | 385.84 K/s | 1.47 µs | 1.98 µs | 8.03 µs | 77.95 µs | 181.25 µs | 72.02 ms | 11575273 |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 10.00 K/s | 60 ns | 95 ns | 192 ns | 3.12 µs | 13.33 µs | 1.88 ms | 300000 |
| 256 | 10.00 K/s | 55 ns | 147 ns | 1.42 µs | 4.58 µs | 19.20 µs | 2.61 ms | 300000 |
| 4k | 10.00 K/s | 1.53 µs | 2.84 µs | 8.10 µs | 16.88 µs | 90.94 µs | 3.96 ms | 300000 |


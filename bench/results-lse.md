# xchannel benchmark — `lse`

_Generated 2026-04-30T14:32:30+01:00 by `bench/run.sh`._

## System

```
host:        lse
kernel:      Linux 5.14.0-570.37.1.el9_6.x86_64 #1 SMP PREEMPT_DYNAMIC Sat Aug 16 01:10:00 EDT 2025 x86_64 GNU/Linux
os:          Red Hat Enterprise Linux 9.6 (Plow)
cpu:         Intel(R) Xeon(R) Gold 6146 CPU @ 3.20GHz
cores:       2 logical
numa nodes:  2
page size:   4096 bytes
rustc:       unknown
cmdline:     BOOT_IMAGE=(hd0,gpt2)/vmlinuz-5.14.0-570.37.1.el9_6.x86_64 root=/dev/mapper/vg--root-root ro nofb quiet splash=quiet crashkernel=1G-4G:192M,4G-64G:256M,64G-:512M rd.lvm.lv=vg-root/root rd.lvm.lv=vg-root/swap rd.lvm.lv=vg-root/usr nosoftlockup ipv6.disable=1 intel_idle.max_cstate=0 intel_pstate=disable mce=ignore_ce idle=poll audit=0 skew_tick=1 transparent_hugepage=never isolcpus=1-11,13-23 irqaffinity=0,12 nohz=on nohz_full=1-11,13-23 rcu_nocb_poll rcu_nocbs=1-11,13-23 console=tty0 console=ttyS0,115200n8

writer core: 4
reader core: 3
duration:    30s + 3s warmup per cell
region size: 16m
roll size:   1g
keep files:  2
disk path:   ~/xchannel-bench/disk (xfs)
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
| 64 | 5.82 M/s | 108 ns | 137.85 µs | 400.64 µs | 614.40 µs | 735.23 µs | 91.95 ms | 174723612 |
| 256 | 3.54 M/s | 269 ns | 373.25 µs | 503.81 µs | 616.96 µs | 704.00 µs | 92.67 ms | 106091385 |
| 4k | 317.18 K/s | 899 ns | 483.58 µs | 584.19 µs | 671.23 µs | 934.40 µs | 92.27 ms | 9515406 |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1.00 M/s | 74 ns | 131 ns | 306 ns | 561.15 µs | 691.71 µs | 91.49 ms | 30000001 |
| 256 | 1.00 M/s | 114 ns | 251.13 µs | 444.42 µs | 603.13 µs | 692.74 µs | 91.68 ms | 30000000 |
| 4k | 314.43 K/s | 896 ns | 478.21 µs | 583.17 µs | 670.21 µs | 934.40 µs | 92.14 ms | 9432960 |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 10.00 K/s | 90 ns | 132 ns | 159 ns | 344 ns | 448 ns | 7.88 ms | 300000 |
| 256 | 10.00 K/s | 117 ns | 216 ns | 379 ns | 456 ns | 654.85 µs | 7.88 ms | 300000 |
| 4k | 10.00 K/s | 723 ns | 862 ns | 924 ns | 647.17 µs | 878.59 µs | 11.54 ms | 300000 |

### Backend: `disk` (`~/xchannel-bench/disk`)

**Publish gap: `sat`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 5.21 M/s | 91 ns | 188 ns | 2.03 µs | 423.68 µs | 559.10 µs | 89.00 ms | 156213639 |
| 256 | 2.73 M/s | 213 ns | 2.06 µs | 10.32 µs | 446.21 µs | 567.81 µs | 88.80 ms | 81814394 |
| 4k | 231.76 K/s | 2.82 µs | 3.06 µs | 59.71 µs | 474.11 µs | 593.92 µs | 89.33 ms | 6952749 |

**Publish gap: `1 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1.00 M/s | 77 ns | 135 ns | 169 ns | 385.54 µs | 548.86 µs | 87.62 ms | 30000000 |
| 256 | 1.00 M/s | 127 ns | 2.05 µs | 2.32 µs | 439.04 µs | 571.39 µs | 88.87 ms | 30000000 |
| 4k | 231.74 K/s | 2.81 µs | 3.04 µs | 57.53 µs | 471.81 µs | 605.18 µs | 88.74 ms | 6952225 |

**Publish gap: `100 µs`**

| msg size | rate | p50 | p90 | p95 | p99 | p99.9 | max | samples |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 10.00 K/s | 91 ns | 134 ns | 168 ns | 2.89 µs | 3.39 µs | 4.19 ms | 300000 |
| 256 | 10.00 K/s | 164 ns | 230 ns | 2.92 µs | 3.17 µs | 8.46 µs | 4.21 ms | 300000 |
| 4k | 10.00 K/s | 2.83 µs | 3.30 µs | 3.42 µs | 454.14 µs | 630.78 µs | 8.11 ms | 300000 |


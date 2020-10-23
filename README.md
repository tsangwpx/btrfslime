# defrag
`btrfslime.defrag` is a frontend tool aware of shared extent

```text
usage: btrfslime.defrag [-h] [--verbose] [--dry-run] [--extent-size EXTENT_SIZE]
                        [--small-extent-size SMALL_EXTENT_SIZE]
                        [--large-extent-size LARGE_EXTENT_SIZE]
                        [--shared-size SHARED_SIZE] [--tolerance TOLERANCE]
                        [--repetitions REPETITIONS] [--flush] [--speed SPEED]
                        root [root ...]

positional arguments:
  root                  Files / directories that need defragment

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v
  --dry-run, -n
  --extent-size EXTENT_SIZE
                        Target extent size (default: 128MB)
  --small-extent-size SMALL_EXTENT_SIZE
                        Minimum size of acceptable extents (default: 32MB)
  --large-extent-size LARGE_EXTENT_SIZE
                        extent smaller than this will be considered (default:
                        128MB)
  --shared-size SHARED_SIZE
                        shared extent smaller than this will be considered
                        (default: 1MB)
  --tolerance TOLERANCE
                        tolerable fraction of fragmented size / extents
                        (default: 0.1)
  --repetitions REPETITIONS
                        defragment multiple times (default: 1)
  --flush
  --speed SPEED         Disk IO speed that estimate pause between files
                        (default: 0, no pause)
```

# dedupe
`btrfslime.dedupe` performs a specialized deduplication of files with equal size.
The equal-size assumption avoids hashing unnecessary files and occupying large space for file hashes.
This tool should be helpful for media file deduplication.

```
usage: btrfslime.dedupe [-h] [--verbose] [--db-file DB_FILE] [--skip-scan]
                        [--skip-hash] [--reset-done] [--no-purge-missing]
                        roots [roots ...]

positional arguments:
  roots               Files / directories that need dedupe

optional arguments:
  -h, --help          show this help message and exit
  --verbose, -v
  --db-file DB_FILE   Cache database file for file hashes (default: dedupe.db)
  --skip-scan         Skip file scanning phase
  --skip-hash         Skip file hashing phase
  --reset-done        Reset the done flag
  --no-purge-missing  Do not purge missing entries from the database

```

 
# defrag
`btrfslime.defrag` is a frontend tool aware of shared extent

```text
usage: btrfslime.defrag [-h] [--verbose] [--dry-run]
                        [--target-size EXTENT_SIZE]
                        [--acceptable-size SMALL_EXTENT_SIZE]
                        [--large-extent-size LARGE_EXTENT_SIZE]
                        [--shared-size SHARED_SIZE] [--tolerance TOLERANCE]
                        [--dedupe] [--repetitions REPETITIONS] [--flush]
                        [--speed SPEED]
                        root [root ...]

positional arguments:
  root                  Files / directories that need defragment

optional arguments:
  -h, --help            show this help message and exit
  --verbose, -v
  --dry-run, -n
  --target-size EXTENT_SIZE
                        Target extent size (default: 128MB)
  --acceptable-size SMALL_EXTENT_SIZE
                        Acceptable extent size (default: 32MB)
  --large-extent-size LARGE_EXTENT_SIZE
                        Ignorable extent size (default: 64MB)
  --shared-size SHARED_SIZE
                        Ignorable shared extent size (default: 1MB)
  --tolerance TOLERANCE
                        tolerable fraction in both size and number of extents
                        (default: 0.1)
  --dedupe              use ioctl FIDEDUPERANGE to defrag hopefully
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

```text
usage: btrfslime.dedupe [-h] [--verbose] [--db-file DB_FILE] [--skip-scan]
                        [--skip-hash] [--skip-dedupe] [--reset-done]
                        [--reset-hash] [--no-purge-missing]
                        roots [roots ...]

positional arguments:
  roots               Files / directories that need dedupe

optional arguments:
  -h, --help          show this help message and exit
  --verbose, -v
  --db-file DB_FILE   Cache database file for file hashes (default: dedupe.db)
  --skip-scan         Skip file scanning phase
  --skip-hash         Skip file hashing phase
  --skip-dedupe       Skip dedupe phase
  --reset-done        Reset the done flag
  --reset-hash        Reset the computed hash values
  --no-purge-missing  Do not purge missing entries from the database
```

 
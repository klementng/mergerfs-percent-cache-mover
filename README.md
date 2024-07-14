# Mergerfs Cache Mover

A python script to move files from a caching disk to a backing pool with snapshotting awareness.

## How it works

The script is inspired by the [mergerfs.percent-full-mover.py](https://github.com/trapexit/mergerfs/blob/latest-release/tools/mergerfs.percent-full-mover?raw=1) tool for [tiered caching](https://github.com/trapexit/mergerfs?tab=readme-ov-file#tiered-caching) in mergerfs. However, the original script does not work for my use case, where my mergerfs pool which is being snapraided using [snapraid-btrfs](https://github.com/automorphism88/snapraid-btrfs).

As a result, the original tool will cause all files to be moved from cache disk to the backing pool due to the persistent disk usage from the snapshots created by snapraid-btrfs.

This tool is made to tackle this issue by being aware of the snapshots and live disk usage. The script will moves non-snapshotted files to the backing pool, followed by snapshotted files (sorted by atime) till the threshold is met.

## Requirements
The following softwares is required by this script
- python3
- rsync

## Usage

To use the script, the following command can be used:

```sh
python3 percent-cache-mover.py /path/to/CACHE /path/to/BACKING_POOL 0.5
```

Full options of the scripts are as follows:

```sh
usage: percent-cache-mover.py [-h] [--metadata METADATA] [--audit] [--log_level LOG_LEVEL] [--log_file LOG_FILE] cache backing threshold

mergerfs percent cache mover with snapshot awareness

positional arguments:
  cache                 path to CACHE device / pool
  backing               path to BACKING device / pool
  threshold             target disk usage threshold

options:
  -h, --help            show this help message and exit
  --metadata METADATA   path to metadata file, used to cache snapshot files information
  --audit               enable audit mode (i.e. do not move files)
  --log_level LOG_LEVEL
                        set logging level
  --log_file LOG_FILE   set logfile path
```

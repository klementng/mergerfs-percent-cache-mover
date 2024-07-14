#!/usr/bin/python3

import argparse
import json
import logging
import os
import shutil
import subprocess
import time


def load_metadata(path):

    if os.path.exists(path):
        logging.debug(f"Loading existing metadata at '{path}'...")

        try:
            f = open(path)
            metadata = json.load(f)
            f.close()

            return metadata

        except:
            logging.warning("Failed to load existing metadata...")

    logging.debug("Using empty metadata...")
    metadata = {}
    return metadata


def save_metadata(path, metadata: dict):
    logging.debug(f"Saving metadata at '{path}'...")
    with open(path, "w") as f:
        json.dump(metadata, f)

    return metadata


def update_metadata(cache_pool: str, metadata: dict):
    # Snapshots
    snap_main = os.path.join(cache_pool, ".snapshots")

    if os.path.exists(snap_main) and os.path.isdir(snap_main):
        current_snapshots = os.listdir(snap_main)

        logging.debug(
            f"Found snapshot directory at {snap_main} with {current_snapshots} snapshot"
        )

        for n in current_snapshots:
            if n in metadata:
                logging.debug(
                    f"Skipping updating metadata for snapshot ({n})... already exist"
                )
                continue

            logging.debug(f"Updating metadata for snapshot ({n})")

            snap_dir = os.path.join(snap_main, n, "snapshot")  # .snapshots/23/snapshot
            metadata.setdefault(n, {"files": {}, "dirs": {}, "root": snap_dir})

            for current_dir, dirs, filenames in os.walk(snap_dir):

                metadata[n]["dirs"][current_dir] = os.stat(current_dir)

                for fn in filenames:
                    fp = os.path.join(current_dir, fn)

                    if os.path.exists(fp):
                        metadata[n]["files"][fp] = os.stat(fp)

        # Remove old snapshot data
        for k in list(metadata.keys()):
            if k == "0":
                continue

            if k not in current_snapshots:
                logging.debug(f"Removing old metadata for snapshot ({k})")
                metadata.pop(k)

    # Livefs
    logging.debug("Updating metadata for live filesystem...")

    base_dev = os.stat(cache_pool).st_dev
    metadata["0"] = {"files": {}, "dirs": {}, "root": cache_pool}

    for current_dir, dirs, filenames in os.walk(cache_pool):

        metadata["0"]["dirs"][current_dir] = os.stat(current_dir)

        if os.stat(current_dir).st_dev != base_dev:
            dirs[:] = []
            continue

        for fn in filenames:
            fp = os.path.join(current_dir, fn)
            if os.path.exists(fp):
                metadata["0"]["files"][fp] = os.stat(fp)

    return metadata


def main(cache_pool, backing_pool, threshold, metadata_path=None, audit_mode=False):
    assert os.path.exists(cache_pool) and os.path.isdir(cache_pool)
    assert os.path.exists(backing_pool) and os.path.isdir(backing_pool)
    assert isinstance(threshold, float) and threshold > 0
    assert isinstance(audit_mode, bool)
    assert (
        os.stat(cache_pool).st_dev != os.stat(backing_pool).st_dev
    ), "CACHE pool must not be same device as the BACKING pool"

    cache_pool = cache_pool.rstrip("/")
    backing_pool = backing_pool.rstrip("/")

    total_size, used_size, free_size = shutil.disk_usage(cache_pool)

    ratio = used_size / total_size
    if ratio < threshold:
        logging.info(
            f"Not continuing... disk usage: ({ratio:.3f}) < threshold ({threshold:.3f})"
        )
        exit(0)

    logging.info("Loading metadata...")
    if metadata_path is not None:
        metadata = load_metadata(metadata_path)
    else:
        metadata = {}

    logging.info("Updating metadata...")
    update_metadata(cache_pool, metadata)

    if metadata_path is not None:
        logging.info("Saving metadata...")
        save_metadata(metadata_path, metadata)

    live_size = sum(metadata["0"]["files"][p][6] for p in metadata["0"]["files"])
    snap_size = used_size - live_size

    ratio = live_size / total_size
    if ratio < threshold:
        logging.info(
            f"Not continuing... disk usage: ({ratio:.3f}) < threshold ({threshold:.3f})"
        )
        exit(0)

    latest_snap_num = max(metadata)
    live_in_snap = []
    live_not_in_snap = []

    for fp in metadata["0"]["files"]:
        o = (fp, metadata["0"]["files"][fp])

        fp_snap = fp.replace(metadata["0"]["root"], metadata[latest_snap_num]["root"])

        if fp_snap in metadata[latest_snap_num]["files"]:
            live_in_snap.append(o)
        else:
            live_not_in_snap.append(o)

    # sort by atime
    atime_key = lambda t: t[1][7]
    live_not_in_snap = sorted(live_not_in_snap, key=atime_key)
    logging.info(f"Found {len(live_not_in_snap)} files not in latest snapshot")

    live_in_snap = sorted(live_in_snap, key=atime_key)
    logging.info(f"Found {len(live_in_snap)} files in latest snapshot")

    live_files = live_not_in_snap + live_in_snap

    ratio = live_size / total_size
    logging.info(f"Moving files... current usage ({ratio})")

    move_count = 0
    start_size = live_size
    for fp, stats in live_files:

        ratio = live_size / total_size
        if ratio < threshold:
            logging.info(
                f"Completed... live usage: ({ratio:.3f}) < threshold ({threshold:.3f})"
            )

            break

        if os.path.exists(fp):
            rsync_fp = cache_pool + "/." + fp.replace(cache_pool, "")
            logging.debug(f"Moving via rsync {rsync_fp} to {backing_pool}")

            if audit_mode is True:
                logging.info(f"AUDIT: Moving via rsync {rsync_fp} to {backing_pool}")
                metadata["0"]["files"].pop(fp)
                live_size -= stats[6]
                move_count += 1

            else:
                process = subprocess.run(
                    [
                        "rsync",
                        "-axqHAXWESR",
                        "--preallocate",
                        "--remove-source-files",
                        rsync_fp,
                        backing_pool,
                    ]
                )

                if process.returncode == 0:
                    live_size -= stats[6]  # bytes
                    metadata["0"]["files"].pop(fp)
                    move_count += 1
                else:
                    logging.warning(
                        f"rsync failed: {rsync_fp} to {backing_pool}. (code: {process.returncode})"
                    )

        else:
            logging.debug(
                f"{fp} does not exist on filesystem... Removing from metadata"
            )
            live_size -= stats[6]  # bytes
            metadata["0"]["files"].pop(fp)

    else:
        logging.info("Completed... No files remaining")

    logging.info(
        f"Moved {move_count} file(s) with total size of {(start_size-live_size)/1.074e+9:.3f} GiB"
    )

    if metadata_path is not None:
        logging.info("Saving metadata...")
        if audit_mode is False:
            save_metadata(metadata_path, metadata)
        else:
            save_metadata(metadata_path + "_audit.json", metadata)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="mergerfs percent cache mover with snapshot awareness"
    )

    parser.add_argument("cache", help="path to CACHE device / pool", type=str)
    parser.add_argument("backing", help="path to BACKING device / pool", type=str)
    parser.add_argument("threshold", help="target disk usage threshold", type=float)
    parser.add_argument(
        "--metadata",
        help="path to metadata file, used to cache snapshot files information",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--audit",
        help="enable audit mode (i.e. do not move files)",
        action="store_true",
        default=False,
    )

    parser.add_argument("--log_level", help="set logging level", default="INFO")
    parser.add_argument("--log_file", help="set logfile path", default=None)
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(levelname)s - %(funcName)s - %(message)s",
    )
    if args.log_file is not None:
        fn = logging.FileHandler(args.log_file)
        fn.setFormatter(logging.Formatter("%(levelname)s - %(name)s - %(message)s"))
        logging.root.addHandler(fn)

    main(args.cache, args.backing, args.threshold, args.metadata, args.audit)

#!/usr/bin/env python3
"""Scatter-gather over the chunks of one Icechunk array.

init    create the store: array `image`, empty array `blurred`, branch `base`
tile    blur one chunk-aligned tile; write only its region in a forked session
        and save that session to a file (no commit, no repo metadata change)
gather  merge all tile sessions on a fresh branch and make the one commit
verify  compare `blurred` with a single-process blur of `image`

Only init and gather change repo metadata (branches, commits). Tile tasks run
concurrently and only add chunk files plus a small session file, which is why
this is safe on a local filesystem store.
"""
import argparse
import os
import pickle
import shutil
import signal
import sys

import icechunk as ic
import numpy as np
import zarr
from scipy.ndimage import gaussian_filter


def open_repo(path, create=False):
    storage = ic.local_filesystem_storage(path)
    return ic.Repository.create(storage) if create else ic.Repository.open(storage)


def halo(sigma):
    return int(4 * sigma + 0.5)


def cmd_init(a):
    """Idempotent: reuse the store's init commit if present, else (re)build it."""
    message = f"scatter-init size={a.size} chunk={a.chunk} seed={a.seed}"
    if os.path.isdir(a.repo):
        try:
            repo = open_repo(a.repo)
            for c in repo.ancestry(branch="main"):
                if c.message == message:
                    if "base" not in repo.list_branches():
                        repo.create_branch("base", snapshot_id=c.id)
                    print(c.id)
                    return
        except Exception as e:  # unreadable store: rebuild
            print(f"rebuilding {a.repo}: {e}", file=sys.stderr)
        shutil.rmtree(a.repo)
    repo = open_repo(a.repo, create=True)
    session = repo.writable_session("main")
    g = zarr.open_group(session.store, mode="a", zarr_format=3)
    shape, chunks = (a.size, a.size), (a.chunk, a.chunk)
    img = g.create_array("image", shape=shape, chunks=chunks, dtype="uint16")
    g.create_array("blurred", shape=shape, chunks=chunks, dtype="float32", fill_value=-1.0)
    rng = np.random.default_rng(a.seed)
    for r in range(0, a.size, a.chunk):
        img[r:r + a.chunk, :] = rng.integers(0, 4096, size=(min(a.chunk, a.size - r), a.size), dtype=np.uint16)
    snapshot = session.commit(message)
    repo.create_branch("base", snapshot_id=snapshot)
    print(snapshot)


def tile_bounds(a):
    ts = a.tile_chunks * a.chunk
    grid = -(-a.size // ts)
    ti, tj = divmod(a.tile, grid)
    return ti * ts, min((ti + 1) * ts, a.size), tj * ts, min((tj + 1) * ts, a.size)


def cmd_tile(a):
    repo = open_repo(a.repo)
    r0, r1, c0, c1 = tile_bounds(a)
    h = halo(a.sigma)
    R0, R1, C0, C1 = max(r0 - h, 0), min(r1 + h, a.size), max(c0 - h, 0), min(c1 + h, a.size)
    src = zarr.open_group(repo.readonly_session(snapshot_id=a.parent).store, mode="r")["image"]
    block = gaussian_filter(src[R0:R1, C0:C1].astype("float32"), a.sigma, mode="nearest", truncate=4.0)
    core = block[r0 - R0:r1 - R0, c0 - C0:c1 - C0]
    fork = repo.writable_session("base").fork()
    assert fork.snapshot_id == a.parent, f"base branch is at {fork.snapshot_id}, not {a.parent}"
    zarr.open_group(fork.store, mode="a")["blurred"][r0:r1, c0:c1] = core
    # test hook: die once, after the chunks are written (ICE_FAULT_MODE=error exits 3, else SIGKILL)
    faults = [t for t in os.environ.get("ICE_FAULT_TILES", "").split(",") if t]
    if str(a.tile) in faults:
        marker = os.path.join(os.environ["ICE_FAULT_DIR"], f"{os.path.basename(a.repo)}-tile{a.tile}")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        try:
            os.mkdir(marker)
        except FileExistsError:
            pass
        else:
            if os.environ.get("ICE_FAULT_MODE") == "error":
                sys.exit(3)
            os.kill(os.getpid(), signal.SIGKILL)
    with open(a.out, "wb") as fh:
        pickle.dump(fork, fh)


def cmd_gather(a):
    repo = open_repo(a.repo)
    if a.branch in repo.list_branches():
        repo.delete_branch(a.branch)
    repo.create_branch(a.branch, snapshot_id=a.parent)
    session = repo.writable_session(a.branch)
    names = [os.path.basename(p) for p in a.forks]
    assert len(set(names)) == len(names), f"duplicate fork file names: {names}"
    forks = []
    for path in sorted(a.forks):
        with open(path, "rb") as fh:
            forks.append(pickle.load(fh))
    session.merge(*forks)
    print(session.commit(f"gather {len(forks)} tiles"))


def cmd_verify(a):
    repo = open_repo(a.repo)
    g = zarr.open_group(repo.readonly_session(snapshot_id=a.parent).store, mode="r")
    expected = gaussian_filter(g["image"][:].astype("float32"), a.sigma, mode="nearest", truncate=4.0)
    diff = float(np.abs(g["blurred"][:] - expected).max())
    ok = diff < 1e-3
    with open("verify.csv", "w") as fh:
        fh.write(f"id,snapshot,max_abs_diff,ok\n{a.id},{a.parent},{diff:.3g},{ok}\n")
    print(a.parent)
    if not ok:
        sys.exit(f"blurred differs from single-process blur by {diff}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(required=True)
    s = sub.add_parser("init"); s.set_defaults(fn=cmd_init)
    s.add_argument("--repo", required=True)
    s.add_argument("--size", type=int, required=True)
    s.add_argument("--chunk", type=int, required=True)
    s.add_argument("--seed", type=int, required=True)
    s = sub.add_parser("tile"); s.set_defaults(fn=cmd_tile)
    s.add_argument("--repo", required=True)
    s.add_argument("--parent", required=True)
    s.add_argument("--tile", type=int, required=True)
    s.add_argument("--size", type=int, required=True)
    s.add_argument("--chunk", type=int, required=True)
    s.add_argument("--tile-chunks", type=int, required=True)
    s.add_argument("--sigma", type=float, required=True)
    s.add_argument("--out", required=True)
    s = sub.add_parser("gather"); s.set_defaults(fn=cmd_gather)
    s.add_argument("--repo", required=True)
    s.add_argument("--parent", required=True)
    s.add_argument("--branch", required=True)
    s.add_argument("forks", nargs="+")
    s = sub.add_parser("verify"); s.set_defaults(fn=cmd_verify)
    s.add_argument("--repo", required=True)
    s.add_argument("--parent", required=True)
    s.add_argument("--id", required=True)
    s.add_argument("--sigma", type=float, required=True)
    a = p.parse_args()
    a.fn(a)

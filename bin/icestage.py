#!/usr/bin/env python3
"""Icechunk stages for a record-driven Nextflow workflow.

Every stage takes (repo, parent snapshot), commits on its own branch forked from
that snapshot, and prints the new snapshot id on stdout.

The blur, segment and features steps follow the analysis in blur.py, segment.py
and extract_features.py from BioImageTools/ome-zarr-image-analysis-nextflow
(MIT, Copyright (c) 2023 embl-cba), reimplemented against Icechunk sessions
rather than OME-Zarr directories. https://github.com/BioImageTools/ome-zarr-image-analysis-nextflow
"""
import argparse
import sys

import icechunk as ic
import numpy as np
import zarr


def open_repo(path, create=False):
    storage = ic.local_filesystem_storage(path)
    return ic.Repository.create(storage) if create else ic.Repository.open(storage)


def stage_session(repo, parent, branch):
    """Writable session on `branch`, (re)created at `parent` so reruns are idempotent."""
    if branch in repo.list_branches():
        repo.delete_branch(branch)
    repo.create_branch(branch, snapshot_id=parent)
    return repo.writable_session(branch)


def fingerprint(path):
    """Cheap input identity: relative paths + sizes."""
    import hashlib
    from pathlib import Path

    root = Path(path)
    h = hashlib.sha256()
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        h.update(f"{f.relative_to(root)}:{f.stat().st_size}\n".encode())
    return h.hexdigest()[:16]


def cmd_init(a):
    """Idempotent: reuse the store's import commit if present, else (re)build it."""
    import os
    import shutil

    message = f"import {os.path.basename(a.input)} sha={fingerprint(a.input)}"
    if os.path.isdir(a.repo):
        try:
            for c in open_repo(a.repo).ancestry(branch="main"):
                if c.message == message:
                    print(c.id)
                    return
        except Exception as e:  # unreadable store: rebuild
            print(f"rebuilding {a.repo}: {e}", file=sys.stderr)
        shutil.rmtree(a.repo)
    repo = open_repo(a.repo, create=True)
    session = repo.writable_session("main")
    dst = zarr.open_group(session.store, mode="a", zarr_format=3)
    src = zarr.open_group(a.input, mode="r")
    dst.attrs.update(dict(src.attrs))
    for lvl in ("0", "1", "2"):
        s = src[lvl]
        d = dst.create_array(f"image/{lvl}", shape=s.shape, chunks=s.chunks, dtype=s.dtype)
        d[:] = s[:]
    print(session.commit(message))


def cmd_blur(a):
    from skimage.filters import gaussian

    repo = open_repo(a.repo)
    session = stage_session(repo, a.parent, a.branch)
    g = zarr.open_group(session.store, mode="a")
    img = g["image/0"][:]
    sigma = [float(s) for s in a.sigma.split(",")]
    blurred = gaussian(img, sigma=sigma, preserve_range=True).astype(img.dtype)
    out = g.create_array("blurred/0", shape=blurred.shape, chunks=g["image/0"].chunks, dtype=img.dtype)
    out[:] = blurred
    out.attrs["sigma"] = sigma
    print(session.commit(f"blur sigma={a.sigma}"))


def cmd_segment(a):
    from skimage import filters, morphology

    repo = open_repo(a.repo)
    session = stage_session(repo, a.parent, a.branch)
    g = zarr.open_group(session.store, mode="a")
    img = g["blurred/0"][:]
    mask = morphology.label(img > filters.threshold_otsu(img)).astype(np.uint16)
    out = g.create_array(f"labels/{a.name}/0", shape=mask.shape, chunks=g["image/0"].chunks, dtype=mask.dtype)
    out[:] = mask
    print(session.commit(f"segment {a.name}"))


def cmd_features(a):
    from skimage.measure import regionprops_table

    repo = open_repo(a.repo)
    session = stage_session(repo, a.parent, a.branch)
    g = zarr.open_group(session.store, mode="a")
    labels = g[f"labels/{a.name}/0"][:].reshape(labels_2d := g[f"labels/{a.name}/0"].shape[-2:])
    cols = ["label", "area", "eccentricity", "solidity"]
    tbl = regionprops_table(labels, properties=cols)
    arr = np.stack([tbl[c] for c in cols], axis=1).astype("float64")
    out = g.create_array(f"features/{a.name}", shape=arr.shape, dtype=arr.dtype)
    out[:] = arr
    out.attrs["columns"] = cols
    print(session.commit(f"features {a.name}"))


def cmd_export(a):
    import csv

    repo = open_repo(a.repo)
    g = zarr.open_group(repo.readonly_session(snapshot_id=a.parent).store, mode="r")
    arr = g[f"features/{a.name}"]
    with open("features.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(arr.attrs["columns"])
        w.writerows(arr[:].tolist())
    print(a.parent)


def cmd_info(a):
    repo = open_repo(a.repo)
    for c in repo.ancestry(snapshot_id=a.parent):
        print(c.id, c.message, file=sys.stderr)
    g = zarr.open_group(repo.readonly_session(snapshot_id=a.parent).store, mode="r")
    print(g.tree())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(required=True)
    for name, fn, extra in [
        ("init", cmd_init, ["input"]),
        ("blur", cmd_blur, ["sigma"]),
        ("segment", cmd_segment, ["name"]),
        ("features", cmd_features, ["name"]),
        ("export", cmd_export, ["name"]),
        ("info", cmd_info, []),
    ]:
        s = sub.add_parser(name)
        s.set_defaults(fn=fn)
        s.add_argument("--repo", required=True)
        if name != "init":
            s.add_argument("--parent", required=True)
        if name not in ("init", "info", "export"):
            s.add_argument("--branch", required=True)
        for e in extra:
            s.add_argument(f"--{e}" if e != "input" else "--input", required=True)
    a = p.parse_args()
    a.fn(a)

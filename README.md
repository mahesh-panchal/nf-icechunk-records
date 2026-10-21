# nf-icechunk-records

Can a data version control system keep one dataset that stages **add to**, instead of each stage writing a full copy, while Nextflow's `-resume` cache stays valid? This prototype tries it with [Icechunk](https://icechunk.io) (a versioned store for [Zarr](https://zarr.dev) arrays). Requires Nextflow >= 26.10 (typed syntax, records).

## Answer

**It works, but for this example it is not worth the extra machinery.**

- It works: stages add to one store without copying it, resume reuses the cache, parallel tasks can add to it, and a killed task is retried by Nextflow.
- It is not clearly worth it here. On a POSIX filesystem, plain Zarr directories where each stage writes only what it creates, with the next store assembled from hardlinks, used the same disk (83.6 MB against 83.6 MB for one stage, 86.7 MB against 86.6 MB for three) with no extra dependency. Copying the whole store at every stage used 2 to 7 times more.
- A versioned store could still pay off where links are not possible or where plain directories do not fit: object storage, stages that modify existing data in place, many concurrent writers, or a need for exact versions and provenance. That is untested here, and other tools (DVC, lakeFS, Delta Lake) might fit better.

## How it works

A small record `{id, repo, snapshot}` is passed between stages. `repo` is the store's location, `snapshot` is the commit the data is at. Each stage forks a branch at its parent snapshot, commits, and emits a record with the new snapshot id. Task hashes depend only on that short string, so resume works and the data is never staged into task directories.

`main.nf` is the sequential version (one store per sample, stages in sequence):

| Stage | Reads | Writes (commit) |
|---|---|---|
| IMPORT | OME-Zarr in `data/` | `image/` |
| BLUR | `image/0` | `blurred/0` |
| SEGMENT | `blurred/0` | `labels/<name>/0` |
| FEATURES | `labels/<name>/0` | `features/<name>` |
| EXPORT | `features/<name>` | none (CSV published to `results/`) |

`scatter.nf` is the parallel version: many `TILE` tasks each write their own chunks of one array into a shared store in `work/icechunk/`, and one `GATHER` task per sample merges them and makes the only commit.

## Requirements and usage

- [pixi](https://pixi.sh) (provides Nextflow) and Docker. Tasks run in a Wave container built from `envs/icechunk.yml`; rebuild it with `wave --conda-file envs/icechunk.yml --platform linux/amd64 --freeze --await -o json` and update `process.container` in `nextflow.config`.

```
pixi run icechunk-stages          # main.nf: stores in icechunk_stores/, results in results/
pixi run scatter-gather           # scatter.nf: parallel tiles, store in work/icechunk/
pixi run test-resume              # main.nf resume scenarios (scratch copy, needs docker)
pixi run test-scatter             # scatter.nf scenarios (scratch copy, needs docker)
pixi run clean-all                # remove work/, results/, icechunk_stores/ and .nextflow*
```

Add `-resume` to resume a run. Parameters: `--sigma`, `--seg_name`, `--store_dir` for `main.nf`; `--size`, `--chunk`, `--tile_chunks`, `--sigma`, `--samples` for `scatter.nf`. The docker config mounts `icechunk_stores/`; change it in `nextflow.config` if you change `--store_dir`.

## What was tested

Each row is asserted by `tests/resume.sh` or `tests/scatter.sh`, except where noted.

**`main.nf`** (2 samples, 10 tasks):

| Change | Result on `-resume` |
|---|---|
| none | all 10 cached, `IMPORT` included |
| new commit on `main` after import | ignored, all 10 cached |
| a stage snapshot file removed | stops at `checkRef`: snapshot not found, re-run without `-resume` |
| whole store deleted | same error; a run without `-resume` rebuilds it |
| `--sigma` changed | BLUR onward reruns for both samples (8 run, 2 cached) |
| same-size edit to an input file | **not noticed**, all 10 cached (checked by hand, not in the test) |

**`scatter.nf`** (2 samples x 16 tiles, 38 tasks):

| Scenario | Result |
|---|---|
| tiles 3 and 7 SIGKILLed once each, after writing | Nextflow retries them (4 failed attempts); result equals a single-process blur exactly |
| `-resume`, nothing changed | all 38 cached |
| one tile's work dir deleted | that tile, `GATHER` and `VERIFY` rerun (3 tasks) |
| a tile fails for good, then `-resume` | resume finishes and reuses the completed tiles |
| concurrency | up to 10 tiles ran at the same time |
| project (`work/` and store) moved | all 38 cached |
| store deleted | resume fails with `checkRef`; a run without `-resume` rebuilds it |

Where the data lives, for a 4096 x 4096 sample with `scatter.nf`: the store was 83 MB and all task directories together were 0.5 MB. Each task keeps only its own small files (a 182 to 908 byte session file per tile, `verify.csv`); only the store is shared.

## Disk use against the usual way

Measured once with a throwaway baseline workflow that is not kept in this repository: the same job with the Zarr store passed between processes as a plain directory, as nf-core modules such as spatialvi's `sdata/*` do. Size of `work/` after one clean run (1 sample, 4096 x 4096, 16 tiles per stage):

| Version | One stage | Three stages (blur, segment, features) |
|---|---|---|
| shared Icechunk store | 83.6 MB | 86.6 MB |
| plain directories, hardlinked assembly | 83.6 MB | 86.7 MB |
| plain directories, each stage copies the store | 166.6 MB (tiles return only their pixels), 614.7 MB (tiles return full copies) | 338.9 MB |

Hardlinks need one filesystem, stages that never rewrite a chunk they received, and tiles that cover whole chunks of an unsharded array. They do not work on an object store.

## Findings worth knowing before building on this

- **The cache cannot see inside the store.** Edits made after import, corrupt chunks, and same-size edits to input files go unnoticed (even with `deep`). A check on every launch (`checkRef`) catches a deleted snapshot or store, but not changed contents. Corruption only shows when a stage reads the data.
- **One committer on a local filesystem.** Letting every tile commit to one branch lost writes silently: in 10 trials with 8 processes, every commit returned success but 30 of 80 tiles were missing. Concurrent branch create and delete also lost updates. So only `INIT` and `GATHER` change repo metadata, and tiles hand back sessions to merge. Per-process commits should be safe on an object store with conditional writes (untested).
- **Distinct names for collected files.** Four `Set<Path>` inputs all named `tile.fork` were staged under one name, so `GATHER` merged one tile four times. Tile files are named `tile-<n>.fork` and `gather` refuses duplicates.
- **Moving the project reruns every task unless `process.cache = 'deep'`** (set in `nextflow.config`). The default cache key includes the absolute path of each `bin/` script, so a moved or re-cloned project reran all 38 tasks; `deep` hashes content instead and it resumed fully cached. This is ordinary Nextflow behaviour, not specific to Icechunk.
- **Keep absolute paths out of scripts and records** or the task hash changes when the project moves. `main.nf` still uses an absolute `--store_dir`; `scatter.nf` uses a key relative to `work/`.
- **Typed helper functions break `-resume`.** Do not give `checkRef`-style helpers typed record params or returns; every task reruns. Reported upstream: [nextflow-io/nextflow#7884](https://github.com/nextflow-io/nextflow/issues/7884).
- **`checkRef` errors are masked.** They show as `InvocationTargetException` on the console; the message is in `.nextflow.log`.
- **Store lifetime.** `scatter.nf` keeps the store in `work/`, so purging `work/` removes the data. Retried tasks leave unreferenced chunks in the store, and old snapshots need garbage collection.
- **Not tested:** an object store (S3, MinIO), a shared filesystem across nodes, arrays larger than a few GB, and per-process commits on a store with conditional writes. Sizes above are from one machine's Docker mount.

## Credits

> [!NOTE]
> The dummy data and the image-analysis steps (blur, segment, features) come from [BioImageTools/ome-zarr-image-analysis-nextflow](https://github.com/BioImageTools/ome-zarr-image-analysis-nextflow). This prototype reworks that idea to pass an Icechunk store between stages by records.
>
> That repository is MIT licensed (Copyright (c) 2023 embl-cba). The images in `data/` are copied from it; its licence text is in [`data/NOTICE.md`](data/NOTICE.md). `bin/icestage.py` and `main.nf` carry attribution headers.

## References

- [Icechunk](https://icechunk.io): transactional storage engine for Zarr
- [Zarr](https://zarr.dev): chunked, compressed N-dimensional arrays
- [OME-Zarr (OME-NGFF)](https://ngff.openmicroscopy.org): bioimaging data layout on Zarr
- [Nextflow](https://nextflow.io/docs/latest/): typed syntax and records
- [Seqera Wave](https://seqera.io/wave/): container build service used for `envs/icechunk.yml`
- [pixi](https://pixi.sh): environment and task runner

## Licence

[MIT](LICENSE). The images in `data/` are third-party, see [`data/NOTICE.md`](data/NOTICE.md).

# nf-icechunk-records

Can a data version control system keep one dataset that stages add to, instead of each stage writing a full copy, while Nextflow's `-resume` cache stays valid? This prototype tries it with [Icechunk](https://icechunk.io), a versioned store for [Zarr](https://zarr.dev) arrays. Requires Nextflow >= 26.10 (typed syntax, records).

## Answer

**It works, but for this example it is not worth the extra machinery.**

- Stages add to one store without copying it, `-resume` reuses the cache, parallel tasks can add to it, and Nextflow retries killed tasks.
- On a POSIX filesystem, plain Zarr directories gave the same disk use with no extra dependency. Each stage wrote only what it created, and the next store was assembled from hardlinks (see [Disk use](#disk-use-against-the-usual-way)). Copying the whole store at every stage used 2 to 7 times more.
- A versioned store could still pay off where links are impossible or plain directories do not fit: object storage, in-place changes to existing data, many concurrent writers, exact versions and provenance. Untested here. Other tools (DVC, lakeFS, Delta Lake) might fit better.

## How it works

A record `{id, repo, snapshot}` passes between stages. `repo` is the store's location and `snapshot` is the commit the data is at. Each stage forks a branch at its parent snapshot, commits, and emits a record with the new snapshot id. Task hashes depend only on that short string, so resume works and the data is never staged into task directories.

`main.nf` runs the stages in sequence, one store per sample:

| Stage | Reads | Writes (commit) |
|---|---|---|
| IMPORT | OME-Zarr in `data/` | `image/` |
| BLUR | `image/0` | `blurred/0` |
| SEGMENT | `blurred/0` | `labels/<name>/0` |
| FEATURES | `labels/<name>/0` | `features/<name>` |
| EXPORT | `features/<name>` | none (CSV published to `results/`) |

`scatter.nf` runs in parallel. Many `TILE` tasks write their own chunks of one array into a shared store in `work/icechunk/`. One `GATHER` task per sample merges them and makes the only commit.

## Usage

Requires [pixi](https://pixi.sh) and Docker. Tasks run in a Wave container built from `envs/icechunk.yml`. To rebuild it, run `wave --conda-file envs/icechunk.yml --platform linux/amd64 --freeze --await -o json` and update `process.container` in `nextflow.config`.

```
pixi run icechunk-stages          # main.nf: stores in icechunk_stores/, results in results/
pixi run scatter-gather           # scatter.nf: parallel tiles, store in work/icechunk/
pixi run test-resume              # main.nf resume scenarios (scratch copy, needs docker)
pixi run test-scatter             # scatter.nf scenarios (scratch copy, needs docker)
pixi run clean-all                # remove work/, results/, icechunk_stores/ and .nextflow*
```

Add `-resume` to resume. Parameters: `--sigma`, `--seg_name`, `--store_dir` (`main.nf`); `--size`, `--chunk`, `--tile_chunks`, `--sigma`, `--samples` (`scatter.nf`). If you change `--store_dir`, change the store mount under `docker` in `nextflow.config` too.

## What was tested

`tests/resume.sh` and `tests/scatter.sh` assert each row except where noted.

**`main.nf`** (2 samples, 10 tasks):

| Change | Result on `-resume` |
|---|---|
| none | all 10 cached, `IMPORT` included |
| new commit on `main` after import | ignored, all 10 cached |
| a stage snapshot file removed | stops at `checkRef`: snapshot not found |
| whole store deleted | same error; a run without `-resume` rebuilds it |
| `--sigma` changed | BLUR onward reruns for both samples (8 run, 2 cached) |
| same-size edit to an input file | **not noticed**, all 10 cached (checked by hand) |

**`scatter.nf`** (2 samples x 16 tiles, 38 tasks):

| Scenario | Result |
|---|---|
| tiles 3 and 7 SIGKILLed once each, after writing | Nextflow retries them (4 failed attempts); result equals a single-process blur exactly |
| `-resume`, nothing changed | all 38 cached |
| one tile's work dir deleted | that tile, `GATHER` and `VERIFY` rerun (3 tasks) |
| a tile fails for good, then `-resume` | resume finishes and reuses the completed tiles |
| concurrency | up to 10 tiles ran at once |
| project (`work/` and store) moved | all 38 cached |
| store deleted | resume fails at `checkRef`; a run without `-resume` rebuilds it |

For a 4096 x 4096 sample, the `scatter.nf` store was 83 MB and all task directories together were 0.5 MB. A task keeps only its own small files (a 182 to 908 byte session file per tile, `verify.csv`); only the store is shared.

## Disk use against the usual way

The baseline is a throwaway workflow that is not kept in this repository. It ran the same job with the store passed between processes as a plain directory, as nf-core modules such as spatialvi's `sdata/*` do. Size of `work/` after one clean run (1 sample, 4096 x 4096, 16 tiles per stage):

| Version | One stage | Three stages (blur, segment, features) |
|---|---|---|
| shared Icechunk store | 83.6 MB | 86.6 MB |
| plain directories, hardlinked assembly | 83.6 MB | 86.7 MB |
| plain directories, each stage copies the store | 166.6 MB | 338.9 MB |
| as above, and each tile returns a full store copy | 614.7 MB | not run |

Hardlinks need one filesystem, stages that never rewrite a chunk they received, and tiles that cover whole chunks of an unsharded array. They do not work on an object store.

## Findings

- **The cache cannot see inside the store.** Edits after import, corrupt chunks, and same-size edits to input files go unnoticed, even with `process.cache = 'deep'`. `checkRef` runs on every launch and catches a deleted snapshot or store, not changed contents. Corruption shows only when a stage reads the data.
- **One committer on a local filesystem.** With every tile committing to one branch, 8 processes in 10 trials returned success on every commit but lost 30 of 80 tiles. Concurrent branch create and delete also lost updates. So only `INIT` and `GATHER` change repo metadata, and tiles hand back sessions to merge. Per-process commits should be safe on an object store with conditional writes (untested).
- **Collected files need distinct names.** Four `Set<Path>` inputs named `tile.fork` were staged under one name, so `GATHER` merged one tile four times. Tile files are named `tile-<n>.fork` and `gather` refuses duplicates.
- **Moving the project reruns every task unless the hash is path-free.** The default cache key includes the absolute path of each `bin/` script, so a moved or re-cloned project reran all 38 tasks. `process.cache = 'deep'` (set in `nextflow.config`) hashes content instead, and the moved project resumed fully cached. This is ordinary Nextflow behaviour, not specific to Icechunk. Absolute paths in scripts or records break it too: `main.nf` puts an absolute `--store_dir` in its script, so it reruns when moved. `scatter.nf` uses a key relative to `work/`.
- **Typed helper functions break `-resume`.** Give `checkRef`-style helpers no typed record params or returns, or every task reruns. Reported upstream: [nextflow-io/nextflow#7884](https://github.com/nextflow-io/nextflow/issues/7884).
- **`checkRef` errors are masked.** The console shows `InvocationTargetException`; the message is in `.nextflow.log`.
- **Store lifetime.** `scatter.nf` keeps its store in `work/`, so purging `work/` removes the data. Retried tasks leave unreferenced chunks, and old snapshots need garbage collection.
- **Not tested:** object stores (S3, MinIO), a filesystem shared across nodes, arrays over a few GB, and per-process commits with conditional writes. Sizes are from one machine's Docker mount.

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

# nf-icechunk-records

Prototype: modify one [Icechunk](https://icechunk.io) datastore through several Nextflow stages without breaking `-resume` and without a copy of the store in every work directory. Requires Nextflow >= 26.10 (typed syntax, records).

## Idea

A record `{id, repo, snapshot}` is passed between stages. `repo` is a path outside `work/`; `snapshot` is the commit marking the stage the data is at. Each stage forks a branch at its parent snapshot, commits, and emits a record with the new snapshot id. Task hashes depend only on that short string, so resume works.

| Stage | Reads | Writes (commit) |
|---|---|---|
| IMPORT | OME-Zarr in `data/` | `image/` |
| BLUR | `image/0` | `blurred/0` |
| SEGMENT | `blurred/0` | `labels/<name>/0` |
| FEATURES | `labels/<name>/0` | `features/<name>` |
| EXPORT | `features/<name>` | none (CSV published to `results/`) |

## Requirements

- [pixi](https://pixi.sh) (provides Nextflow >= 26.10)
- Docker. Tasks run in a Wave container, `community.wave.seqera.io/library/icechunk:02b6b7323726df2f`, built from `envs/icechunk.yml` (`linux/amd64`, frozen, no expiry). To rebuild: `wave --conda-file envs/icechunk.yml --platform linux/amd64 --freeze --await -o json`, then update `process.container` in `nextflow.config`.

## Usage

    pixi run run                      # stores in icechunk_stores/, results in results/
    pixi run run --sigma 1,1,1,4,4    # reruns BLUR onward only
    pixi run lint                     # nextflow lint main.nf
    pixi run clean-work               # drop work/ of earlier runs, keep stores and results
    pixi run clean-all                # remove work/, results/, icechunk_stores/ and .nextflow*

| Parameter | Default | Meaning |
|---|---|---|
| `--store_dir` | `icechunk_stores/` | Where the per-sample Icechunk repos live |
| `--sigma` | `1,1,1,2.5,2.5` | Gaussian sigma per axis for BLUR |
| `--seg_name` | `otsu` | Label and feature table name |

Outputs: `results/features/<id>/features.csv` and `results/tables.csv` (index with the snapshot each table came from).

The docker config mounts `icechunk_stores/` into the container because the stores live outside `work/`. If you change `--store_dir`, change `docker.runOptions` in `nextflow.config` to match.

Dummy data: the two OME-Zarr images from [BioImageTools/ome-zarr-image-analysis-nextflow](https://github.com/BioImageTools/ome-zarr-image-analysis-nextflow).

## Resume safety

- `IMPORT` is `cache false` but idempotent: an intact store returns its existing import snapshot (downstream stays cached); a missing store is rebuilt and only that sample's chain reruns.
- `checkRef` verifies `<repo>/snapshots/<id>` exists before each stage on every launch, including `-resume`.

## Caveats

- Local filesystem storage is not safe for concurrent commits; there is one repo per sample. A shared repo needs an object store.
- Do not give `checkRef`-style helper functions typed record params or returns (`Ref -> Ref`): the record becomes a `Main.Ref` hashed by identity and every task reruns each launch.
- `checkRef` errors show as `InvocationTargetException` on the console; the message is in `.nextflow.log`.
- The check only tests snapshot existence, not chunk integrity; the import fingerprint uses paths and sizes, not contents.

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

## Run

    pixi run run                      # stores in icechunk_stores/, results in results/
    pixi run run --sigma 1,1,1,4,4    # reruns BLUR onward only

Dummy data: the two OME-Zarr images from [BioImageTools/ome-zarr-image-analysis-nextflow](https://github.com/BioImageTools/ome-zarr-image-analysis-nextflow).

## Resume safety

- `IMPORT` is `cache false` but idempotent: an intact store returns its existing import snapshot (downstream stays cached); a missing store is rebuilt and only that sample's chain reruns.
- `checkRef` verifies `<repo>/snapshots/<id>` exists before each stage on every launch, including `-resume`.

## Caveats

- Local filesystem storage is not safe for concurrent commits; there is one repo per sample. A shared repo needs an object store.
- Do not give `checkRef`-style helper functions typed record params or returns (`Ref -> Ref`): the record becomes a `Main.Ref` hashed by identity and every task reruns each launch.
- `checkRef` errors show as `InvocationTargetException` on the console; the message is in `.nextflow.log`.
- The check only tests snapshot existence, not chunk integrity; the import fingerprint uses paths and sizes, not contents.

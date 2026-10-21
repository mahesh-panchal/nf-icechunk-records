// Scatter-gather over the chunks of one Icechunk array, with the store inside
// the work directory (work/icechunk/<id>.icechunk).
//
// Records carry only a store key relative to the work dir, so task hashes do not
// depend on where the project lives. Tiles run in parallel and write only their
// own chunks; one GATHER task per sample merges them and makes the only commit.
nextflow.enable.types = true

params {
    size: Integer = 2048          // array is size x size
    chunk: Integer = 256
    tile_chunks: Integer = 2      // a tile is tile_chunks x tile_chunks chunks
    sigma: Float = 3.0
    samples: Integer = 2
    fault_tiles: String = ''      // e.g. '3,7': those tiles die once, after writing their chunks
    fault_mode: String = 'kill'   // 'kill' = SIGKILL (retried), 'error' = exit 3 (fails the run)
}

record Sample {
    id: String
    seed: Integer
}

record Ref {
    id: String
    repo: String
    snapshot: String
}

record Tile {
    id: String
    repo: String
    snapshot: String
    tile: Integer
}

record Fork {
    id: String
    repo: String
    snapshot: String
    tile: Integer
    fork: Path
}

record Group {
    id: String
    repo: String
    snapshot: String
    forks: Set<Path>
}

record Report {
    id: String
    snapshot: String
    report: Path
}

// Cached like any process. If it reruns it is idempotent: it reuses the init commit when
// the store already holds these settings, else it rebuilds the store. A store deleted
// behind the cache is caught by checkRef.
process INIT {
    tag "${sample.id}"

    input:
    sample: Sample

    script:
    repo = "icechunk/${sample.id}.icechunk"
    """
    ROOT=\$(dirname "\$(dirname "\$PWD")")
    SNAPSHOT=\$(icescatter.py init --repo "\$ROOT/${repo}" \\
        --size ${params.size} --chunk ${params.chunk} --seed ${sample.seed})
    """

    output:
    record(id: sample.id, repo: repo, snapshot: env('SNAPSHOT'))
}

process TILE {
    tag "${t.id}:${t.tile}"

    input:
    t: Tile

    script:
    """
    ROOT=\$(dirname "\$(dirname "\$PWD")")
    ICE_FAULT_TILES='${params.fault_tiles}' ICE_FAULT_MODE='${params.fault_mode}' ICE_FAULT_DIR="\$ROOT/icechunk/.faults" \\
    icescatter.py tile --repo "\$ROOT/${t.repo}" --parent ${t.snapshot} --tile ${t.tile} \\
        --size ${params.size} --chunk ${params.chunk} --tile-chunks ${params.tile_chunks} \\
        --sigma ${params.sigma} --out tile-${t.tile}.fork
    """

    output:
    record(id: t.id, repo: t.repo, snapshot: t.snapshot, tile: t.tile, fork: file("tile-${t.tile}.fork"))
}

process GATHER {
    tag "${g.id}"

    input:
    g: Group

    script:
    """
    ROOT=\$(dirname "\$(dirname "\$PWD")")
    SNAPSHOT=\$(icescatter.py gather --repo "\$ROOT/${g.repo}" --parent ${g.snapshot} \\
        --branch gather-${g.snapshot} ${g.forks.join(' ')})
    """

    output:
    record(id: g.id, repo: g.repo, snapshot: env('SNAPSHOT'))
}

process VERIFY {
    tag "${ref.id}"

    input:
    ref: Ref

    script:
    """
    ROOT=\$(dirname "\$(dirname "\$PWD")")
    icescatter.py verify --repo "\$ROOT/${ref.repo}" --parent ${ref.snapshot} \\
        --id ${ref.id} --sigma ${params.sigma} > /dev/null
    """

    output:
    record(id: ref.id, snapshot: ref.snapshot, report: file('verify.csv'))
}

// Runs on every launch, including -resume, where cached tasks are skipped and nothing
// else would notice a deleted or rewritten store. Not typed on purpose: a helper with a
// typed record param or return makes every task rerun (nextflow-io/nextflow#7884).
def checkRef(ref) {
    def snap = file("${workflow.workDir}/${ref.repo}/snapshots/${ref.snapshot}")
    if( !snap.exists() )
        error "Icechunk snapshot ${ref.snapshot} not found in ${workflow.workDir}/${ref.repo} (sample '${ref.id}'). " +
              "The store was removed or changed after it was created; re-run without -resume."
    return ref
}

workflow {
    main:
    per_side = (params.size + params.tile_chunks * params.chunk - 1).intdiv(params.tile_chunks * params.chunk)
    ntiles = per_side * per_side

    ch_samples = channel.fromList((1..params.samples).toList())
        .map { n -> record(id: "sample${n}", seed: n) }
    ch_ref = INIT(ch_samples)
    ch_tiles = ch_ref.map { ref -> checkRef(ref) }.flatMap { ref ->
        (0..<ntiles).collect { i -> record(id: ref.id, repo: ref.repo, snapshot: ref.snapshot, tile: i) }
    }
    ch_forks = TILE(ch_tiles)
    ch_gathered = GATHER(
        ch_forks
            .map { f -> tuple(f.id, f) }
            .groupBy()
            .map { id, fs -> record(id: id, repo: fs.toList()[0].repo, snapshot: fs.toList()[0].snapshot, forks: fs.collect { f -> f.fork }.toSet()) }
    )
    ch_report = VERIFY(ch_gathered.map { ref -> checkRef(ref) })

    publish:
    reports = ch_report
}

output {
    reports: Channel<Report> {
        path { r -> r.report >> "scatter/${r.id}/" }
        index {
            path 'scatter.csv'
            header true
        }
    }
}

// Icechunk datastore threaded through stages by a record {id, repo, snapshot}.
// The store lives outside work/ (params.store_dir); tasks only receive its
// location and a snapshot id, so cache hashes stay small and stable.
nextflow.enable.types = true

params {
    store_dir: String = "${projectDir}/icechunk_stores"
    sigma: String = '1,1,1,2.5,2.5'
    seg_name: String = 'otsu'
}

record Src {
    id: String
    zarr: Path
}

record Ref {
    id: String
    repo: String
    snapshot: String
}

record Table {
    id: String
    snapshot: String
    features: Path
}

// Idempotent and never cached: it returns the existing import snapshot when the
// store is intact (downstream stays cached) and rebuilds it when it is gone.
process IMPORT {
    tag "${src.id}"
    cache false

    input:
    src: Src

    output:
    record(id: src.id, repo: repo, snapshot: stdout().trim())

    script:
    repo = "${params.store_dir}/${src.id}.icechunk"
    """
    icestage.py init --repo ${repo} --input ${src.zarr}
    """
}

process BLUR {
    tag "${ref.id}"

    input:
    ref: Ref
    sigma: String

    output:
    record(id: ref.id, repo: ref.repo, snapshot: stdout().trim())

    script:
    """
    icestage.py blur --repo ${ref.repo} --parent ${ref.snapshot} \\
        --branch blur-${ref.snapshot} --sigma ${sigma}
    """
}

process SEGMENT {
    tag "${ref.id}"

    input:
    ref: Ref
    name: String

    output:
    record(id: ref.id, repo: ref.repo, snapshot: stdout().trim())

    script:
    """
    icestage.py segment --repo ${ref.repo} --parent ${ref.snapshot} \\
        --branch segment-${ref.snapshot} --name ${name}
    """
}

process FEATURES {
    tag "${ref.id}"

    input:
    ref: Ref
    name: String

    output:
    record(id: ref.id, repo: ref.repo, snapshot: stdout().trim())

    script:
    """
    icestage.py features --repo ${ref.repo} --parent ${ref.snapshot} \\
        --branch features-${ref.snapshot} --name ${name}
    """
}

// Read-only: materialise a table from a snapshot (nothing is committed).
process EXPORT {
    tag "${ref.id}"

    input:
    ref: Ref
    name: String

    output:
    record(id: ref.id, snapshot: ref.snapshot, features: file('features.csv'))

    script:
    """
    icestage.py export --repo ${ref.repo} --parent ${ref.snapshot} --name ${name} > /dev/null
    """
}

// Runs on every launch, including -resume, where cached tasks are skipped and
// nothing else would notice a deleted or rewritten store.
def checkRef(ref) {
    def snap = file("${ref.repo}/snapshots/${ref.snapshot}")
    if( !snap.exists() )
        error "Icechunk snapshot ${ref.snapshot} not found in ${ref.repo} (sample '${ref.id}'). " +
              "A stage store was modified after IMPORT; re-run without -resume."
    return ref
}

workflow ICE_STAGES {
    take:
    samples: Channel<Src>
    sigma: String
    name: String

    main:
    ch_imp = IMPORT(samples)
    ch_blur = BLUR(ch_imp.map { ref -> checkRef(ref) }, sigma)
    ch_seg = SEGMENT(ch_blur.map { ref -> checkRef(ref) }, name)
    ch_feat = FEATURES(ch_seg.map { ref -> checkRef(ref) }, name)
    ch_table = EXPORT(ch_feat.map { ref -> checkRef(ref) }, name)

    emit:
    tables: Channel<Table> = ch_table
}

workflow {
    main:
    ch_in = channel.fromPath("data/*.ome.zarr", type: 'dir', checkIfExists: true)
        .map { z -> record(id: z.name.replace('.ome.zarr', ''), zarr: z) }

    ice = ICE_STAGES(ch_in, params.sigma, params.seg_name)

    publish:
    tables = ice
}

output {
    tables: Channel<Table> {
        path { t -> t.features >> "features/${t.id}/" }
        index {
            path 'tables.csv'
            header true
        }
    }
}

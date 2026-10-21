#!/usr/bin/env bash
# Resume scenarios for the Icechunk record workflow.
#
# nf-test cannot express these: it gives each test a fresh work dir and runs
# Nextflow once (an open PR adds a resume option: askimed/nf-test#339). So this
# drives `nextflow run -resume` directly, in a scratch copy of the project, and
# asserts on the completed/cached counts. Needs nextflow and docker.
set -uo pipefail

SRC=$(cd "$(dirname "$0")/.." && pwd)
T=$(mktemp -d "${TMPDIR:-/tmp}/resume-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

tar -C "$SRC" --exclude=work --exclude='.nextflow*' --exclude=results \
    --exclude=icechunk_stores --exclude=.pixi --exclude=.git -cf - . | tar -C "$T" -xf -
cd "$T" || exit 1

IMAGE=$(nextflow config -flat 2>/dev/null | sed -n "s/^process.container = '\(.*\)'$/\1/p")
[ -n "$IMAGE" ] || { echo "cannot read process.container from nextflow.config"; exit 1; }
CONTROL=icechunk_stores/xy_8bit__nuclei_PLK1_control.icechunk

fails=0
LOG=$T/run.log

# run <args...>: nextflow run -resume, sets RC, COMPLETED, CACHED
run() {
    nextflow run main.nf -resume "$@" >"$LOG" 2>&1
    RC=$?
    COMPLETED=$(sed -n 's/.*completed=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
    CACHED=$(sed -n 's/.*cached=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
}

# ice <python>: run python with icechunk/zarr from the workflow's container
ice() {
    docker run --rm --platform linux/amd64 -v "$T/icechunk_stores:/work/icechunk_stores" \
        -w /work "$IMAGE" python -c "$1" 2>/dev/null
}

# check <name> <expected completed> <expected cached>
check() {
    if [ "$RC" -eq 0 ] && [ "$COMPLETED" = "$2" ] && [ "$CACHED" = "$3" ]; then
        echo "PASS  $1 (completed=$COMPLETED cached=$CACHED)"
    else
        echo "FAIL  $1: expected completed=$2 cached=$3, got rc=$RC completed=${COMPLETED:-?} cached=${CACHED:-?}"
        fails=$((fails + 1))
    fi
}

run
check "fresh run" 10 0

run
check "resume, nothing changed (only IMPORT reruns)" 2 8

ice "
import icechunk as ic, zarr
r = ic.Repository.open(ic.local_filesystem_storage('$CONTROL'))
s = r.writable_session('main')
zarr.open_group(s.store, mode='a')['image/0'][..., :50, :50] = 0
s.commit('manual edit after import')
" >/dev/null || { echo "FAIL  could not commit the manual edit"; exit 1; }
run
check "new commit on main is not noticed, everything stays cached" 2 8

BLUR_SNAP=$(ice "
import icechunk as ic
r = ic.Repository.open(ic.local_filesystem_storage('$CONTROL'))
print(next(c.id for b in r.list_branches() for c in r.ancestry(branch=b) if c.message.startswith('blur')))
")
rm "$CONTROL/snapshots/$BLUR_SNAP"
run
if [ "$RC" -ne 0 ] && grep -q "Icechunk snapshot $BLUR_SNAP not found" .nextflow.log; then
    echo "PASS  removed intermediate snapshot is caught by checkRef"
else
    echo "FAIL  removed intermediate snapshot: expected checkRef error for $BLUR_SNAP, got rc=$RC"
    fails=$((fails + 1))
fi

rm -rf "$CONTROL"
run
check "deleted store: IMPORT rebuilds, only that sample reruns" 6 4

run --sigma 1,1,1,4,4
check "sigma change reruns BLUR onward for every sample" 10 0

[ "$fails" -eq 0 ] && echo "all resume scenarios passed" || echo "$fails scenario(s) failed"
exit "$fails"

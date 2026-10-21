#!/usr/bin/env bash
# Scatter-gather scenarios for scatter.nf: parallel tiles writing one Icechunk
# store in the work directory, with failures handled by Nextflow's own retry
# and resume. Runs in a scratch copy of the project. Needs nextflow and docker.
set -uo pipefail

SRC=$(cd "$(dirname "$0")/.." && pwd)
T=$(mktemp -d "${TMPDIR:-/tmp}/scatter-test.XXXXXX")
T2=${T}-moved
trap 'chmod -R u+w "$T" "$T2" 2>/dev/null; rm -rf "$T" "$T2"' EXIT

tar -C "$SRC" --exclude=work --exclude='.nextflow*' --exclude=results \
    --exclude=icechunk_stores --exclude=.pixi --exclude=.git -cf - . | tar -C "$T" -xf -
cd "$T" || exit 1

ARGS=(--samples 2 --size 2048 --chunk 256)   # 2 samples x 16 tiles
ARGS1=(--samples 1 --size 2048 --chunk 256)
NTASKS=38                                     # 2 INIT + 32 TILE + 2 GATHER + 2 VERIFY
fails=0
LOG=$T/run.log
cat > trace.config <<'C'
trace { raw = true; fields = 'name,status,start,realtime' }
C

run() {
    nextflow run scatter.nf -resume -with-trace -c trace.config "$@" >"$LOG" 2>&1
    RC=$?
    COMPLETED=$(sed -n 's/.*completed=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
    CACHED=$(sed -n 's/.*cached=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
}
verified() { # every sample's reconstruction equals a single-process blur
    local n; n=$(cat results/scatter/*/verify.csv 2>/dev/null | grep -c ',True$')
    [ "$n" = "${1:-2}" ]
}
report() { # report <name> <ok:0|1> <detail>
    if [ "$2" -eq 0 ]; then echo "PASS  $1 ($3)"; else echo "FAIL  $1 ($3)"; fails=$((fails + 1)); fi
}

# 1. tiles 3 and 7 are SIGKILLed once each (after writing chunks); Nextflow retries them
run "${ARGS[@]}" --fault_tiles 3,7
killed=$(awk -F'\t' 'NR>1 && $1 ~ /./ && $0 ~ /TILE/ && $0 ~ /FAILED/' trace-*.txt 2>/dev/null | wc -l | tr -d ' ')
ok=1; [ "$RC" -eq 0 ] && verified && [ "$killed" = "4" ] && ok=0
report "killed tiles are retried by Nextflow and the result is still exact" $ok "rc=$RC killed_attempts=$killed completed=$COMPLETED"

# 2. nothing changed
run "${ARGS[@]}" --fault_tiles 3,7
ok=1; [ "$RC" -eq 0 ] && verified && [ "$COMPLETED" = "0" ] && [ "$CACHED" = "$NTASKS" ] && ok=0
report "resume: every task is cached, INIT included" $ok "rc=$RC completed=$COMPLETED cached=$CACHED"

# 3. one tile's work dir lost: that tile, its GATHER and VERIFY rerun
wd=$(nextflow log last -f name,workdir 2>/dev/null | awk -F'\t' '$1 ~ /TILE \(sample1:5\)/ {print $2}' | head -1)
[ -n "$wd" ] && rm -rf "$wd"
run "${ARGS[@]}" --fault_tiles 3,7
ok=1; [ "$RC" -eq 0 ] && verified && [ "$COMPLETED" = "3" ] && ok=0
report "resume after one lost tile: that tile, GATHER and VERIFY rerun" $ok "rc=$RC completed=$COMPLETED cached=$CACHED"

# 4. a tile fails for good mid-run (exit 3 is not retried), then resume finishes the job
rm -rf work .nextflow* results
run "${ARGS1[@]}" --fault_tiles 15 --fault_mode error
first_rc=$RC
run "${ARGS1[@]}" --fault_tiles 15 --fault_mode error
ok=1; [ "$first_rc" -ne 0 ] && [ "$RC" -eq 0 ] && verified 1 && [ "$CACHED" -gt 0 ] && ok=0
report "failure then resume: finishes, tiles done before the failure are reused" $ok "first_rc=$first_rc resume_rc=$RC completed=$COMPLETED cached=$CACHED"

# 5. several tiles really ran at the same time
max=$(python3 - <<'P'
import csv, glob
ev = []
for f in glob.glob("trace-*.txt"):
    for r in csv.DictReader(open(f), delimiter="\t"):
        if "TILE" in r["name"] and r["status"] == "COMPLETED" and r["realtime"].isdigit():
            s = int(r["start"]) / 1000
            ev += [(s, 1), (s + int(r["realtime"]) / 1000, -1)]
cur = best = 0
for _, d in sorted(ev, key=lambda e: (e[0], e[1])):
    cur += d; best = max(best, cur)
print(best)
P
)
ok=1; [ "${max:-0}" -ge 2 ] && ok=0
report "tiles overlap in time" $ok "max concurrent tiles=${max:-?}"

# 6. the whole project (work dir and store included) moves; the store key is relative
rm -rf work .nextflow* results trace-*.txt
run "${ARGS[@]}" --fault_tiles 3,7
cp -Rp "$T" "$T2"
( cd "$T2" && nextflow run scatter.nf -resume "${ARGS[@]}" --fault_tiles 3,7 >"$LOG" 2>&1; echo "rc=$?" >>"$LOG" )
moved_completed=$(sed -n 's/.*completed=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
moved_cached=$(sed -n 's/.*cached=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
ok=1; [ "$moved_completed" = "0" ] && [ "$moved_cached" = "$NTASKS" ] && ok=0
report "moved project resumes with every task cached" $ok "completed=$moved_completed cached=$moved_cached"

# 7. the store is deleted behind the cache: resume fails loudly, a run without -resume rebuilds it
cd "$T" || exit 1
rm -rf work/icechunk/sample1.icechunk
run "${ARGS[@]}" --fault_tiles 3,7
ok=1; [ "$RC" -ne 0 ] && grep -q "Icechunk snapshot .* not found" .nextflow.log && ok=0
report "deleted store: resume fails with a checkRef error" $ok "rc=$RC"
nextflow run scatter.nf -with-trace -c trace.config "${ARGS[@]}" --fault_tiles 3,7 >"$LOG" 2>&1
RC=$?; COMPLETED=$(sed -n 's/.*completed=\([0-9]*\).*/\1/p' "$LOG" | tail -1)
ok=1; [ "$RC" -eq 0 ] && verified && [ "$COMPLETED" = "$NTASKS" ] && ok=0
report "without -resume the store is rebuilt and everything reruns" $ok "rc=$RC completed=$COMPLETED"

[ "$fails" -eq 0 ] && echo "all scatter scenarios passed" || echo "$fails scatter scenario(s) failed"
exit "$fails"

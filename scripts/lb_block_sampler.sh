#!/usr/bin/env bash
# Samples the Letterboxd block rate from the Railway egress IP over time.
# Each tick fires N /scrape-user probes through the live worker and records
# how many got blocked (403/AccessDenied). Appends one line per tick to the
# log. Stop with: kill <pid>  (pid printed at launch).
#
# Tuning via env: INTERVAL (sec, default 1800), PROBES (default 2),
# MAX_TICKS (default 24 -> ~12h), WORKER (default Railway), LBU (default leo604).
set -u
cd "$(dirname "$0")/.."
set -a && . ./.env && set +a

INTERVAL="${INTERVAL:-1800}"
PROBES="${PROBES:-2}"
MAX_TICKS="${MAX_TICKS:-24}"
WORKER="${WORKER:-https://moviemaestro-production.up.railway.app}"
LBU="${LBU:-leo604}"
TESTER="00000000-0000-0000-0000-000000000000"
LOG="scripts/lb_block_samples.log"
SB=(-H "apikey: $SUPABASE_SERVICE_ROLE_KEY" -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY")

CUM_PASS=0; CUM_BLOCK=0; CUM_OTHER=0
echo "# sampler start $(date -u +%FT%TZ) worker=$WORKER probes=$PROBES interval=${INTERVAL}s ticks=$MAX_TICKS" >> "$LOG"

probe () {
  local JOB IC RESULT ROW i
  JOB=$(uuidgen | tr 'A-Z' 'a-z')
  IC=$(curl -sS -X POST "$SUPABASE_URL/rest/v1/user_scrape_jobs" "${SB[@]}" \
        -H "Content-Type: application/json" -H "Prefer: return=minimal" \
        -d "{\"id\":\"$JOB\",\"lbusername\":\"$LBU\",\"status\":\"running\",\"started_by\":\"$TESTER\"}" \
        -o /dev/null -w "%{http_code}")
  [ "$IC" != "201" ] && { echo "OTHER"; return; }
  curl -sS -m 12 -X POST "$WORKER/scrape-user" \
        -H "Authorization: Bearer $WORKER_SHARED_SECRET" -H "Content-Type: application/json" \
        -d "{\"job_id\":\"$JOB\",\"lbusername\":\"$LBU\"}" -o /dev/null
  RESULT="timeout"
  for i in $(seq 1 25); do
    sleep 3
    ROW=$(curl -sS "$SUPABASE_URL/rest/v1/user_scrape_jobs?id=eq.$JOB&select=status,errors,log_tail" "${SB[@]}")
    RESULT=$(printf '%s' "$ROW" | python3 -c '
import sys,json
d=json.load(sys.stdin)
if not d: print("MISSING"); sys.exit()
r=d[0]; s=r["status"]
if s not in ("completed","failed","cancelled"): print("running"); sys.exit()
blob=(json.dumps(r.get("errors"))+(r.get("log_tail") or "")).lower()
print("BLOCK" if (("accessdenied" in blob) or ("403" in blob) or ("blocked" in blob)) else ("PASS" if s=="completed" else "OTHER"))')
    case "$RESULT" in running) continue;; *) break;; esac
  done
  curl -sS -X PATCH "$SUPABASE_URL/rest/v1/user_scrape_jobs?id=eq.$JOB&status=eq.running" "${SB[@]}" \
       -H "Content-Type: application/json" -d '{"status":"failed"}' -o /dev/null
  case "$RESULT" in PASS|BLOCK) echo "$RESULT";; *) echo "OTHER";; esac
}

for tick in $(seq 1 "$MAX_TICKS"); do
  P=0; B=0; O=0
  for _ in $(seq 1 "$PROBES"); do
    case "$(probe)" in PASS) P=$((P+1));; BLOCK) B=$((B+1));; *) O=$((O+1));; esac
  done
  CUM_PASS=$((CUM_PASS+P)); CUM_BLOCK=$((CUM_BLOCK+B)); CUM_OTHER=$((CUM_OTHER+O))
  TOT=$((CUM_PASS+CUM_BLOCK))
  RATE="n/a"; [ "$TOT" -gt 0 ] && RATE=$(python3 -c "print(f'{$CUM_BLOCK/$TOT*100:.1f}%')")
  echo "$(date -u +%FT%TZ) tick=$tick pass=$P block=$B other=$O | cum_pass=$CUM_PASS cum_block=$CUM_BLOCK cum_block_rate=$RATE" >> "$LOG"
  curl -sS -X DELETE "$SUPABASE_URL/rest/v1/user_scrape_jobs?started_by=eq.$TESTER" "${SB[@]}" -o /dev/null
  [ "$tick" -lt "$MAX_TICKS" ] && sleep "$INTERVAL"
done
echo "# sampler done $(date -u +%FT%TZ) cum_pass=$CUM_PASS cum_block=$CUM_BLOCK cum_block_rate=$RATE" >> "$LOG"

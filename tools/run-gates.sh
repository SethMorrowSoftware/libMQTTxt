#!/usr/bin/env bash
# run-gates.sh - every compiler-free check this repo has, in the order that
# makes each one's result mean something.
#
# OXT has no headless way to compile or run a `.oxtstack` / `.livecodescript`,
# so nothing here can prove the library LOADS or the demo BUILDS ITS WINDOW.
# What these gates prove is everything that is pure computation or pure
# structure - and the ordering below is the point:
#
#   THE DISCRIMINATING TESTS RUN FIRST. A gate that has gone blind reports OK,
#   and OK is exactly what a blind gate and a clean tree look like from the
#   outside. The mutation tests reintroduce each defect and fail if the gate
#   stays quiet, so they are what make the OKs underneath worth reading.
#
# Usage: tools/run-gates.sh
set -uo pipefail

cd "$(dirname "$0")/.."

fail=0
run() {
   local label="$1"; shift
   printf '\n=== %s ===\n' "$label"
   if "$@"; then
      return 0
   fi
   printf '!!! FAILED: %s\n' "$label"
   fail=1
}

# --- the gates that make the other gates mean something ----------------------
run "gate discrimination: library"  python3 tools/test-check-libmqttxt.py
run "gate discrimination: demo"     python3 tools/test-demo-gates.py

# --- the library --------------------------------------------------------------
run "library static gate"           python3 tools/check-libmqttxt.py
run "MQTT 3.1.1 protocol vectors"   python3 tools/test-mqtt-vectors.py

# --- the demo -----------------------------------------------------------------
run "embedded library is current"   python3 tools/sync-demo-embeds.py --check
run "carried blocks are unchanged"  python3 tools/check-carried-blocks.py
run "control lists are derived"     python3 tools/check-demo-control-lists.py
run "layout fits its window"        python3 tools/check-demo-layout.py
run "delayed handlers pin a stack"  python3 tools/check-timer-stack-pin.py

printf '\n'
if [ "$fail" -ne 0 ]; then
   echo "run-gates: FAILURES above"
   exit 1
fi
echo "run-gates: all gates green"
echo
echo "NOTE: none of this observes an engine. Everything in this repo is"
echo "'verified statically; needs an OXT pass' until somebody runs it."

#!/bin/bash
WT=/Users/espg/software/zagg/.claude/worktrees/agent-ad587cf9811759e50
cd $WT
# wait for NEON (already running) to produce its result
until [ -f "$WT/results_neon.json" ]; do sleep 10; done
echo "NEON_DONE"
uv run --with h5py python scripts_measure_option_a.py rgt1336 "$WT/results_rgt1336.json" >> "$WT/rgt_run.log" 2>&1
echo "RGT_DONE"

#!/usr/bin/env bash
set -euo pipefail

# Path to db_bench binary
DB_BENCH="/home/eros/forRTC/oadoc/db_bench"

# YCSB workload files directory
WORKLOAD_DIR="/home/eros/forRTC/oadoc/ycsb_workload"

# Where to store DB instances and logs (fresh DB per use_adoc value)
DB_ROOT="/mnt/new1/db_ycsb/1124-1/"
LOG_DIR="/home/eros/forRTC/oadoc/result/log_ycsb-1124-1/"
# Workloads and parameters
load_workload="workloada"          # used for initial load
run_workloads=(workloada workloadb workloadd workloadf workloadc)
threads=1
load_num=64000000                  # records to load once
running_num=64000000              # ops per workload run
request_speed=200                   # MB/s
stats_interval=1
configs=(
  "name=ark ark=1 tea=0 fea=0"
  "name=adoc ark=0 tea=1 fea=1"
  # "name=baseline ark=0 tea=0 fea=0"
)

mkdir -p "$LOG_DIR"

timestamp="$(date +%Y%m%d-%H%M%S)"

if [[ ! -x "$DB_BENCH" ]]; then
  echo "db_bench not found at $DB_BENCH" >&2
  exit 1
fi

load_workload_path="${WORKLOAD_DIR}/${load_workload}"
if [[ ! -f "$load_workload_path" ]]; then
  echo "Load workload file not found: $load_workload_path" >&2
  exit 1
fi

for cfg in "${configs[@]}"; do
  eval "$cfg"
  db_path="${DB_ROOT}_${name}_${timestamp}"
  # db_path="/mnt/new1/db_ycsb/1122-3/_adoc_20251123-085552"
  # db_path="/mnt/new1/db_ycsb/1122-3/_ark_20251122-235608"
  # db_path="/mnt/new1/db_ycsb/1124/_baseline_20251124-104721"

  # Fresh DB for load
  rm -rf "$db_path"
  mkdir -p "$db_path"

  load_log="${LOG_DIR}/load_ycsb_${load_workload}_t${threads}_${name}_${timestamp}_load.log"
  load_report="${load_log}.report.csv"

  load_cmd=(
    "$DB_BENCH"
    --benchmarks=ycsb_load,stats
    --db="$db_path"
    --running_num="$running_num"
    --load_num="$load_num"
    --value_size=1000
    --compression_type=none
    --ycsb_workload="$load_workload_path"
    --ycsb_request_speed="$request_speed"
    --statistics
    --ARK_enable="$ark"
    --TEA_enable="$tea"
    --FEA_enable="$fea"
    --stats_interval_seconds="$stats_interval"
    --report_interval_seconds="$stats_interval"
    --report_file="$load_report"
    --histogram
  )

  echo "Running load (${name}): ${load_cmd[*]}" | tee "$load_log"
  "${load_cmd[@]}" >> "$load_log" 2>&1

  echo "Starting run workloads on same DB: $db_path (${name})"
  for workload in "${run_workloads[@]}"; do
    workload_path="${WORKLOAD_DIR}/${workload}"
    if [[ ! -f "$workload_path" ]]; then
      echo "Workload file not found: $workload_path" >&2
      continue
    fi

    run_log="${LOG_DIR}/ycsb_${workload}_t${threads}_${name}_${timestamp}_run.log"
    run_report="${run_log}.report.csv"

    run_cmd=(
      "$DB_BENCH"
      --benchmarks=ycsb_run,stats
      --db="$db_path"
      --use_existing_db=1
      --running_num="$running_num"
      --load_num="$load_num"
      --ycsb_workload="$workload_path"
      --ycsb_request_speed="$request_speed"
      --statistics
      --value_size=1000
      --compression_type=none
      --ARK_enable="$ark"
      --TEA_enable="$tea"
      --FEA_enable="$fea"
      --stats_interval_seconds="$stats_interval"
      --report_interval_seconds="$stats_interval"
      --report_file="$run_report"
      --histogram
    )

    echo "Running run (${name}): ${run_cmd[*]}" | tee "$run_log"
    "${run_cmd[@]}" >> "$run_log" 2>&1
    echo "Finished workload=${workload}, log=${run_log}"
  done

done

echo "All runs complete. DBs at ${DB_ROOT}_*_${timestamp}, logs in $LOG_DIR"

#!/usr/bin/env bash
set -euo pipefail

configs=(
  "name=ark ark=1 tea=0 fea=0"
  "name=adoc ark=0 tea=1 fea=1"
  "name=baseline ark=0 tea=0 fea=0"
)

bench_value="1000"
bench_compression="none"
bench_key="16"
bench_benchmarks="fillrandom,stats,sstables"
bench_nums=(300000000)
number_of_runs=1

current_time=$(date +"%d-%H-%M")
log_file_dir="../result/write-${current_time}/"
mkdir -p "$log_file_dir"

bench_file_path="../../db_bench"

if [[ ! -x "${bench_file_path}" ]]; then
  echo "Error: ${bench_file_path} not found or not executable"
  exit 1
fi

for num in "${bench_nums[@]}"; do
  for cfg in "${configs[@]}"; do
    eval "$cfg"
    for round in $(seq 1 $number_of_runs); do
      # db_path="${db_base}/${name}/round${round}"
      # mkdir -p "$db_path"
      log_file="${log_file_dir}/out_${num}_${name}_${round}.log"

      sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' || true
      echo "[INFO] Running num=${num}, config=${name}, round=${round}" | tee -a "$log_file"
      # --db="${db_path}" \
      
      "${bench_file_path}" \
        --key_size="${bench_key}" \
        --value_size="${bench_value}" \
        --benchmarks="${bench_benchmarks}" \
        --num="${num}" \
        --compression_type="${bench_compression}" \
        --statistics \
        --stats_interval_seconds=1 \
        --report_interval_seconds=1 \
        --report_file="${log_file}.r.csv" \
        --ARK_enable="${ark}" \
        --TEA_enable="${tea}" \
        --FEA_enable="${fea}" \
        --histogram \
        >>"$log_file" 2>&1
    done
  done
done

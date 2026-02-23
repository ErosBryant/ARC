#!/usr/bin/env bash
set -e

tea=(0)
fea=(0)
silk_bandwidths=(200)
bench_value="1000"
bench_compression="none"
bench_key="16"
bench_benchmarks="fillrandom,stats,sstables"
bench_nums=(200000000)
threads=(2)
unis=(1)
use_rtc=(0)
number_of_runs=1
report_interval_seconds=1

source "/home/eros/forRTC/adoc/trace-zhao/monitor.sh"

current_time=$(date +"%d-%H-%M")
bench_db_base="/mnt/new1/adoc-evaluation/silk/${current_time}"
output_dir="/home/eros/forRTC/new-middle/adoc/cpu/"
log_root="/home/eros/forRTC/new-middle/silk/${current_time}"
mkdir -p "$log_root"
mkdir -p "$output_dir"
summary_log="${output_dir}silk_cpu_mem_totals_${current_time}.csv"
echo "elapsed_seconds,use_rtc,uni,num,thread,bw_limit_mb,round,cpu_percent,user_seconds,sys_seconds,max_rss_kb" > "$summary_log"

bench_file_path="/home/eros/forRTC/oadoc/db_bench"
if [ ! -f "${bench_file_path}" ]; then
  echo "Error: ${bench_file_path} not found!"
  exit 1
fi

for num in "${bench_nums[@]}"; do
  for uni in "${unis[@]}"; do
    for utea in "${tea[@]}"; do
      for ufea in "${fea[@]}"; do
        for bw_limit in "${silk_bandwidths[@]}"; do
          for thread in "${threads[@]}"; do
            for round in $(seq 1 $number_of_runs); do
              for rtc in "${use_rtc[@]}"; do
                run_dir="${log_root}/${num}m"
                mkdir -p "$run_dir"
                log_file=${run_dir}/out_${num}silk${uni}_t${utea}_b${ufea}_rtc${rtc}_thread${thread}_bw${bw_limit}_round${round}.log
                bench_file=${bench_db_base}/${num}silk${uni}_t${utea}_b${ufea}_rtc${rtc}_thread${thread}_bw${bw_limit}_round${round}

                rm -rf "$bench_file" && mkdir -p "$bench_file"

                const_params=" \
                  --db=$bench_file \
                  --key_size=$bench_key \
                  --value_size=$bench_value \
                  --benchmarks=$bench_benchmarks \
                  --num=$num \
                  --compression_type=$bench_compression \
                  --statistics \
                  --stats_interval_seconds=1 \
                  --report_file=${log_file}.r.csv \
                  --report_interval_seconds=1 \
                  --SILK_triggered=${uni} \
                  --SILK_bandwidth_limitation=${bw_limit} \
                  --TEA_enable=${utea} \
                  --FEA_enable=${ufea} \
                  --histogram "

                sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
                echo "[INFO] Running round $round, silk=$uni, rtc=$rtc, t${utea}_b${ufea}_bw${bw_limit} threads=$thread" | tee -a "$log_file"

                start_time=$(date +%s)
                cmd="$bench_file_path $const_params"
                echo "$cmd" >> "$log_file"

                tmp_time="$(mktemp)"
                monitor_prefix=${run_dir}/monitor_${num}silk${uni}_t${utea}_b${ufea}_rtc${rtc}_bw${bw_limit}_round${round}

                bash -c "$cmd" >> "$log_file" 2>&1 &
                bench_pid=$!

                start_monitors "$bench_pid" "$monitor_prefix" "$bench_file" "sdb"

                /usr/bin/time -v -o "$tmp_time" tail --pid="$bench_pid" -f /dev/null

                stop_monitors "$monitor_prefix"

                end_time=$(date +%s)
                elapsed=$(( end_time - start_time ))

                cpu_percent=$(awk -F': *' '/Percent of CPU this job got/ {gsub("%","",$2); print $2}' "$tmp_time")
                user_sec=$(awk -F': *' '/User time \(seconds\)/ {print $2}' "$tmp_time")
                sys_sec=$(awk -F': *' '/System time \(seconds\)/ {print $2}' "$tmp_time")
                max_rss=$(awk -F': *' '/Maximum resident set size/ {print $2}' "$tmp_time")

                [ -z "$cpu_percent" ] && cpu_percent="0"
                [ -z "$user_sec" ] && user_sec="0"
                [ -z "$sys_sec" ] && sys_sec="0"
                [ -z "$max_rss" ] && max_rss="0"

                echo "${elapsed},${rtc},${uni},${num},${thread},${bw_limit},${round},${cpu_percent},${user_sec},${sys_sec},${max_rss}" >> "$summary_log"

                if [[ -d "$bench_file" ]]; then
                  for db_log in "$bench_file"/LOG*; do
                    [[ -e "$db_log" ]] || continue
                    cp "$db_log" "${run_dir}/db_log_${num}silk${uni}_rtc${rtc}_bw${bw_limit}_round${round}_$(basename "$db_log")"
                  done
                  rm -rf "$bench_file"
                fi
                rm -f "$tmp_time"
              done
            done
          done
        done
      done
    done
  done
done

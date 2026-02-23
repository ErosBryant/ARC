#!/usr/bin/env bash
set -e

configs=(
  # "name=ark ark=1 tea=0 fea=0"
  # "name=adoc ark=0 tea=1 fea=1"
  "name=baseline ark=0 tea=0 fea=0"
)

bench_value="1000"
bench_compression="none"
bench_key="16"
bench_benchmarks="fillrandom,stats,sstables"
bench_nums=(250000000)
threads=(2 20)
unis=(1)
use_rtc=(0)
number_of_runs=1

source "/home/eros/forRTC/adoc/trace-zhao/monitor.sh"

current_time=$(date +"%d-%H-%M")
bench_db_base="/mnt/new1/oadoc-evaluation/${current_time}"
output_dir="/home/eros/forRTC/new-middle/oadoc/cpu/"
log_file_dir="/home/eros/forRTC/oadoc/result/write-${current_time}/"
mkdir -p "$log_file_dir"
mkdir -p "$output_dir"
summary_log="${output_dir}${bench_nums}m_cpu_mem_totals_${current_time}.csv"
echo "elapsed_seconds,use_rtc,uni,num,thread,config,round,cpu_percent,user_seconds,sys_seconds,max_rss_kb" > "$summary_log"

bench_file_path="/home/eros/forRTC/oadoc/db_bench"
if [ ! -f "${bench_file_path}" ]; then
  echo "Error: ${bench_file_path} not found!"
  exit 1
fi

for num in "${bench_nums[@]}"; do
  for uni in "${unis[@]}"; do
    for cfg in "${configs[@]}"; do
      eval "$cfg"
      for thread in "${threads[@]}"; do
        for round in $(seq 1 $number_of_runs); do
          for rtc in "${use_rtc[@]}"; do
            log_file=${log_file_dir}out_${num}_${name}_uni${uni}_rtc${rtc}_thread${thread}_round${round}.log
            bench_file="${bench_db_base}${num}_${name}_uni${uni}_rtc${rtc}_thread${thread}_round${round}"

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
              --report_interval_seconds=1 \
              --report_file=${log_file}.r.csv \
              --ARK_enable=${ark} \
              --TEA_enable=${tea} \
              --FEA_enable=${fea} \
              --histogram " 


              # --level0_file_num_compaction_trigger=4 \
            sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
            echo "[INFO] Running round $round, uni=$uni, rtc=$rtc, config=${name}, threads=$thread" | tee -a "$log_file"

             start_time=$(date +%s)
            cmd="$bench_file_path $const_params"
            echo "$cmd" >> "$log_file"

            tmp_time="$(mktemp)"

            # 모니터링 로그 파일 prefix
            monitor_prefix="${log_file_dir}monitor_${num}_${name}_uni${uni}_rtc${rtc}_round${round}"

            # db_bench 실행 + PID 확보
            bash -c "$cmd" >> "$log_file" 2>&1 &
            bench_pid=$!

            # 모니터링 시작
            start_monitors "$bench_pid" "$monitor_prefix" "$bench_file" "sdb"
    

            # 벤치마크 끝날 때까지 기다리면서 time -v로 리소스 기록
            /usr/bin/time -v -o "$tmp_time" tail --pid="$bench_pid" -f /dev/null


            # 모니터링 중단
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

            echo "${elapsed},${rtc},${uni},${num},${thread},${name},${round},${cpu_percent},${user_sec},${sys_sec},${max_rss}" >> "$summary_log"
            if [[ -d "$bench_file" ]]; then
              log_dump_prefix="${log_file_dir}db_log_${num}_${name}_uni${uni}_rtc${rtc}_thread${thread}_round${round}"
              for db_log in "$bench_file"/LOG*; do
                [[ -e "$db_log" ]] || continue
                cp "$db_log" "${log_dump_prefix}_$(basename "$db_log")"
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

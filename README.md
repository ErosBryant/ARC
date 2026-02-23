```
git clone https://github.com/ErosBryant/ARC.git
```

```
make db_bench -j10
```

```
./db_bench --benchmarks=fillrandom,stats   --statistics --stats_interval_seconds=1  --report_interval_seconds=1 --ARK_enable=1 --num=설정 --key_size=설정 --value_size=설정 
```
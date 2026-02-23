//
// Created by jinghuan on 8/24/21.
//

#include <algorithm>
#include <vector>

#include "rocksdb/utilities/report_agent.h"

namespace ROCKSDB_NAMESPACE {

DOTA_Tuner::~DOTA_Tuner() = default;
void DOTA_Tuner::DetectTuningOperations(
    int secs_elapsed, std::vector<ChangePoint> *change_list_ptr) {
  current_sec = secs_elapsed;
  //  UpdateSystemStats();
  SystemScores current_score = ScoreTheSystem();
  UpdateMaxScore(current_score);
  scores.push_back(current_score);
  gradients.push_back(current_score - scores.front());

  auto thread_stat = LocateThreadStates(current_score);
  auto batch_stat = LocateBatchStates(current_score);

  AdjustmentTuning(change_list_ptr, current_score, thread_stat, batch_stat);
  // decide the operation based on the best behavior and last behavior
  // update the histories
  last_thread_states = thread_stat;
  last_batch_stat = batch_stat;
  tuning_rounds++;
}
ThreadStallLevels DOTA_Tuner::LocateThreadStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    // speed is slower than before, performance is in the stall area
    if (score.immutable_number >= 1) {
      if (score.flush_speed_avg <= max_scores.flush_speed_avg * 0.5) {
        // it's not influenced by the flushing speed
        if (current_opt.max_background_jobs > 6) {
          return kBandwidthCongestion;
        }
        //        else {
        //          return kLowFlush;
        //        }
      } else if (score.l0_num > 0.5) {
        // it's in the l0 stall
        return kL0Stall;
      }
    } else if (score.l0_num > 0.7) {
      // it's in the l0 stall
      return kL0Stall;
    } else if (score.estimate_compaction_bytes > 0.5) {
      return kPendingBytes;
    }
  } else if (score.compaction_idle_time > 2.5) {
    return kIdle;
  }
  return kGoodArea;
}

BatchSizeStallLevels DOTA_Tuner::LocateBatchStates(SystemScores &score) {
  if (score.memtable_speed < max_scores.memtable_speed * 0.7) {
    if (score.flush_speed_avg < max_scores.flush_speed_avg * 0.5) {
      if (score.active_size_ratio > 0.5 && score.immutable_number >= 1) {
        return kTinyMemtable;
      } else if (current_opt.max_background_jobs > 6 || score.l0_num > 0.9) {
        return kTinyMemtable;
      }
    }
  } else if (score.flush_numbers < max_scores.flush_numbers * 0.3) {
    return kOverFrequent;
  }

  return kStallFree;
};

SystemScores DOTA_Tuner::ScoreTheSystem() {
  UpdateSystemStats();
  SystemScores current_score;

  uint64_t total_mem_size = 0;
  uint64_t active_mem = 0;
  running_db_->GetIntProperty("rocksdb.size-all-mem-tables", &total_mem_size);
  running_db_->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);

  current_score.active_size_ratio =
      (double)active_mem / (double)current_opt.write_buffer_size;
  current_score.immutable_number =
      cfd->imm() == nullptr ? 0 : cfd->imm()->NumNotFlushed();

  std::vector<FlushMetrics> flush_metric_list;

  auto flush_result_length =
      running_db_->immutable_db_options().flush_stats->size();


  auto compaction_result_length =
      running_db_->immutable_db_options().job_stats->size();

  for (uint64_t i = flush_list_accessed; i < flush_result_length; i++) {
    auto temp = flush_list_from_opt_ptr->at(i);
    current_score.flush_min =
        std::min(current_score.flush_speed_avg, current_score.flush_min);
    flush_metric_list.push_back(temp);
    current_score.flush_speed_avg += temp.write_out_bandwidth;
    current_score.disk_bandwidth += temp.total_bytes;
    last_non_zero_flush = temp.write_out_bandwidth;
    if (current_score.l0_num > temp.l0_files) {
      current_score.l0_num = temp.l0_files;
    }
  }
  int l0_compaction = 0;
  auto num_new_flushes = (flush_result_length - flush_list_accessed);
  current_score.flush_numbers = num_new_flushes;

  while (total_mem_size < last_unflushed_bytes) {
    total_mem_size += current_opt.write_buffer_size;
  }
  current_score.memtable_speed += (total_mem_size - last_unflushed_bytes);

  current_score.memtable_speed /= tuning_gap;
  current_score.memtable_speed /= kMicrosInSecond;  // we use MiB to calculate

  uint64_t max_pending_bytes = 0;

  last_unflushed_bytes = total_mem_size;
  for (uint64_t i = compaction_list_accessed; i < compaction_result_length;
       i++) {
    auto temp = compaction_list_from_opt_ptr->at(i);
    if (temp.input_level == 0) {
      current_score.l0_drop_ratio += temp.drop_ratio;
      l0_compaction++;
    }
    if (temp.current_pending_bytes > max_pending_bytes) {
      max_pending_bytes = temp.current_pending_bytes;
    }
    current_score.disk_bandwidth += temp.total_bytes;
  }

  // flush_speed_avg,flush_speed_var,l0_drop_ratio
  if (num_new_flushes != 0) {
    auto avg_flush = current_score.flush_speed_avg / num_new_flushes;
    current_score.flush_speed_avg /= num_new_flushes;
    for (auto item : flush_metric_list) {
      current_score.flush_speed_var += (item.write_out_bandwidth - avg_flush) *
                                       (item.write_out_bandwidth - avg_flush);
    }
    current_score.flush_speed_var /= num_new_flushes;
    current_score.flush_gap_time /= (kMicrosInSecond * num_new_flushes);
  }

  if (l0_compaction != 0) {
    current_score.l0_drop_ratio /= l0_compaction;
  }
  // l0_num

  current_score.l0_num = (double)(vfs->NumLevelFiles(vfs->base_level())) /
                         current_opt.level0_slowdown_writes_trigger;
  //std::cout << "currenct score, l0 number" << current_score.l0_num <<std::endl;
  //  current_score.l0_num = l0_compaction == 0 ? current_score.l0_num : 0;
  // disk bandwidth,estimate_pending_bytes ratio
  current_score.disk_bandwidth /= kMicrosInSecond;

  current_score.estimate_compaction_bytes =
      (double)vfs->estimated_compaction_needed_bytes() /
      current_opt.soft_pending_compaction_bytes_limit;

  auto flush_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::HIGH);
  auto compaction_thread_idle_list = *env_->GetThreadPoolWaitingTime(Env::LOW);
  std::unordered_map<int, uint64_t> thread_idle_time;
  uint64_t temp = flush_thread_idle_list.size();
  for (uint64_t i = last_flush_thread_len; i < temp; i++) {
    auto temp_entry = flush_thread_idle_list[i];
    auto value = temp_entry.second;
    current_score.flush_idle_time += value;
  }
  temp = compaction_thread_idle_list.size();
  for (uint64_t i = last_compaction_thread_len; i < temp; i++) {
    auto temp_entry = compaction_thread_idle_list[i];
    auto value = temp_entry.second;
    current_score.compaction_idle_time += value;
  }
  current_score.flush_idle_time /=
      (current_opt.max_background_jobs * kMicrosInSecond / 4);
  // flush threads always get 1/4 of all
  current_score.compaction_idle_time /=
      (current_opt.max_background_jobs * kMicrosInSecond * 3 / 4);

  // clean up
  flush_list_accessed = flush_result_length;
  compaction_list_accessed = compaction_result_length;
  last_compaction_thread_len = compaction_thread_idle_list.size();
  last_flush_thread_len = flush_thread_idle_list.size();
  return current_score;
}

void DOTA_Tuner::AdjustmentTuning(std::vector<ChangePoint> *change_list,
                                  SystemScores &score,
                                  ThreadStallLevels thread_levels,
                                  BatchSizeStallLevels batch_levels) {
  // tune for thread number
  auto tuning_op = VoteForOP(score, thread_levels, batch_levels);
  // tune for memtable
  FillUpChangeList(change_list, tuning_op);
}
TuningOP DOTA_Tuner::VoteForOP(SystemScores & /*current_score*/,
                               ThreadStallLevels thread_level,
                               BatchSizeStallLevels batch_level) {
  TuningOP op;
  switch (thread_level) {
      //    case kLowFlush:
      //      op.ThreadOp = kDouble;
      //      break;
    case kL0Stall:
      op.ThreadOp = kLinearIncrease;
      break;
    case kPendingBytes:
      op.ThreadOp = kLinearIncrease;
      break;
    case kGoodArea:
      op.ThreadOp = kKeep;
      break;
    case kIdle:
      op.ThreadOp = kHalf;
      break;
    case kBandwidthCongestion:
      op.ThreadOp = kHalf;
      break;
  }

  if (batch_level == kTinyMemtable) {
    op.BatchOp = kLinearIncrease;
  } else if (batch_level == kStallFree) {
    op.BatchOp = kKeep;
  } else {
    op.BatchOp = kHalf;
  }

  return op;
}

inline void DOTA_Tuner::SetThreadNum(std::vector<ChangePoint> *change_list,
                                     int target_value) {
  ChangePoint thread_num_cp;
  thread_num_cp.opt = max_bg_jobs;
  thread_num_cp.db_width = true;
  target_value = std::max(target_value, min_thread);
  target_value = std::min(target_value, max_thread);
  thread_num_cp.value = std::to_string(target_value);
  change_list->push_back(thread_num_cp);
}

inline void DOTA_Tuner::SetBatchSize(std::vector<ChangePoint> *change_list,
                                     uint64_t memtable_target,
                                     uint64_t sstable_value) {
  ChangePoint memtable_size_cp;
  ChangePoint L1_total_size;
  ChangePoint sst_size_cp;
  //  ChangePoint write_buffer_number;

  sst_size_cp.opt = sst_size;
  L1_total_size.opt = total_l1_size;
  // adjust the memtable size
  memtable_size_cp.db_width = false;
  memtable_size_cp.opt = memtable_size;

  memtable_target = std::max(memtable_target, min_memtable_size);
  memtable_target = std::min(memtable_target, max_memtable_size);
  uint64_t sstable_target =
      sstable_value == 0 ? memtable_target : sstable_value;
  sstable_target = std::max(sstable_target, min_sstable_size);
  sstable_target = std::min(sstable_target, max_sstable_size);

  // SST sizes should be controlled to be the same as memtable size
  memtable_size_cp.value = std::to_string(memtable_target);
  sst_size_cp.value = std::to_string(sstable_target);

  // calculate the total size of L1
  uint64_t l1_size = current_opt.level0_file_num_compaction_trigger *
                     current_opt.min_write_buffer_number_to_merge *
                     memtable_target;

  L1_total_size.value = std::to_string(l1_size);
  sst_size_cp.db_width = false;
  L1_total_size.db_width = false;

  //  change_list->push_back(write_buffer_number);
  change_list->push_back(memtable_size_cp);
  change_list->push_back(L1_total_size);
  change_list->push_back(sst_size_cp);
}

void DOTA_Tuner::FillUpChangeList(std::vector<ChangePoint> *change_list,
                                  TuningOP op) {
  uint64_t current_thread_num = current_opt.max_background_jobs;
  uint64_t current_batch_size = current_opt.write_buffer_size;
  switch (op.BatchOp) {
    case kLinearIncrease:
      SetBatchSize(change_list,
                   current_batch_size += default_opts.write_buffer_size);
      break;
    case kHalf:
      SetBatchSize(change_list, current_batch_size /= 2);
      break;
    case kKeep:
      break;
  }
  switch (op.ThreadOp) {
    case kLinearIncrease:
      SetThreadNum(change_list, current_thread_num += 2);
      break;
    case kHalf:
      SetThreadNum(change_list, current_thread_num /= 2);
      break;
    case kKeep:
      break;
  }
}

SystemScores SystemScores::operator-(const SystemScores &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed - a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio - a.active_size_ratio;
  temp.immutable_number = this->immutable_number - a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg - a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var - a.flush_speed_var;
  temp.l0_num = this->l0_num - a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio - a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes - a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth - a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time - a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time - a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time - a.flush_gap_time;
  temp.flush_numbers = this->flush_numbers - a.flush_numbers;

  return temp;
}

SystemScores SystemScores::operator+(const SystemScores &a) {
  SystemScores temp;
  temp.flush_numbers = this->flush_numbers + a.flush_numbers;
  temp.memtable_speed = this->memtable_speed + a.memtable_speed;
  temp.active_size_ratio = this->active_size_ratio + a.active_size_ratio;
  temp.immutable_number = this->immutable_number + a.immutable_number;
  temp.flush_speed_avg = this->flush_speed_avg + a.flush_speed_avg;
  temp.flush_speed_var = this->flush_speed_var + a.flush_speed_var;
  temp.l0_num = this->l0_num + a.l0_num;
  temp.l0_drop_ratio = this->l0_drop_ratio + a.l0_drop_ratio;
  temp.estimate_compaction_bytes =
      this->estimate_compaction_bytes + a.estimate_compaction_bytes;
  temp.disk_bandwidth = this->disk_bandwidth + a.disk_bandwidth;
  temp.compaction_idle_time =
      this->compaction_idle_time + a.compaction_idle_time;
  temp.flush_idle_time = this->flush_idle_time + a.flush_idle_time;
  temp.flush_gap_time = this->flush_gap_time + a.flush_gap_time;
  return temp;
}

SystemScores SystemScores::operator/(const int &a) {
  SystemScores temp;

  temp.memtable_speed = this->memtable_speed / a;
  temp.active_size_ratio = this->active_size_ratio / a;
  temp.immutable_number = this->immutable_number / a;
  temp.l0_num = this->l0_num / a;
  temp.l0_drop_ratio = this->l0_drop_ratio / a;
  temp.estimate_compaction_bytes = this->estimate_compaction_bytes / a;
  temp.disk_bandwidth = this->disk_bandwidth / a;
  temp.compaction_idle_time = this->compaction_idle_time / a;
  temp.flush_idle_time = this->flush_idle_time / a;

  temp.flush_speed_avg = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_avg / this->flush_numbers;
  temp.flush_speed_var = this->flush_numbers == 0
                             ? 0
                             : this->flush_speed_var / this->flush_numbers;
  temp.flush_gap_time =
      this->flush_numbers == 0 ? 0 : this->flush_gap_time / this->flush_numbers;

  return temp;
}

FEAT_Tuner::~FEAT_Tuner() = default;

void FEAT_Tuner::DetectTuningOperations(int /*secs_elapsed*/,
                                        std::vector<ChangePoint> *change_list) {
  //   first, we tune only when the flushing speed is slower than before
  auto current_score = this->ScoreTheSystem();
  if (current_score.flush_speed_avg == 0) return;
  scores.push_back(current_score);
  if (scores.size() == 1) {
    return;
  }
  this->UpdateMaxScore(current_score);
  if (scores.size() >= (size_t)this->score_array_len) {
    // remove the first record
    scores.pop_front();
  }
  CalculateAvgScore();

  current_score_ = current_score;
  
//  std::cout << current_score_.flush_speed_avg<< std::endl;


  //  std::cout << current_score_.memtable_speed << "/" <<
  //  avg_scores.memtable_speed
  //            << std::endl;

   //<=avg_scores.memtable_speed * TEA_slow_flush) {

  if (current_score_.flush_speed_avg <= 0) {
    return;
  }

  if (ark_enable) {
    TuningOP ark_result = TuneByArk();
    FillUpChangeListArk(change_list, ark_result);
    return;
  }

  TuningOP result{kKeep, kKeep};
  if (TEA_enable) {
    result = TuneByTEA();
  }
  if (FEA_enable) {
    TuningOP fea_result = TuneByFEA();
    result.BatchOp = fea_result.BatchOp;
  }
  FillUpChangeList(change_list, result);
 }

SystemScores FEAT_Tuner::normalize(SystemScores &origin_score) {
  return origin_score;
}



TuningOP FEAT_Tuner::TuneByArk() {
  TuningOP result{kKeep, kKeep, kKeep, kKeep};


  const double flush_baseline =
      avg_scores.flush_speed_avg > 0 ? avg_scores.flush_speed_avg
                                     : max_scores.flush_speed_avg;
  const bool slow_flush =
      current_score_.flush_speed_avg > 0 && flush_baseline > 0 &&
      current_score_.flush_speed_avg <
          flush_baseline * std::min(0.9, TEA_slow_flush + 0.2);
  const bool imm_pressure = current_score_.immutable_number >= 1;
  const bool high_active_ratio = current_score_.active_size_ratio >= 0.5;
  const bool memtable_pressure_now =
      slow_flush || imm_pressure || high_active_ratio;
  constexpr double kL0PressureThreshold = 1.0;
  constexpr double kPendingPressureThreshold = 1.0;
  const bool l0_pressure = current_score_.l0_num >= kL0PressureThreshold;
  const bool pending_pressure =
      current_score_.estimate_compaction_bytes >= kPendingPressureThreshold;
  const bool compaction_pressure_now = pending_pressure || l0_pressure;
  const bool severe_compaction =
      current_score_.estimate_compaction_bytes >= 1.5 ||
      current_score_.l0_num >= 1.2;

  auto accumulate = [](int value, bool condition, int limit) {
    if (condition) {
      value = std::min(value + 1, limit);
    } else if (value > 0) {
      value--;
    }
    return value;
  };

  constexpr int kMaxScore = 4;
  constexpr int kRelaxRounds = 2;
  memtable_pressure_score_ =
      accumulate(memtable_pressure_score_, memtable_pressure_now, kMaxScore);
  compaction_pressure_score_ =
      accumulate(compaction_pressure_score_, compaction_pressure_now, kMaxScore);
  if (memtable_pressure_now) {
    memtable_relax_counter_ = 0;
  } else if (memtable_relax_counter_ < kRelaxRounds) {
    memtable_relax_counter_++;
  }
  if (compaction_pressure_now) {
    compaction_relax_counter_ = 0;
  } else if (compaction_relax_counter_ < kRelaxRounds) {
    compaction_relax_counter_++;
  }

  // 当 flush 速度长期低于近期均值、memtable_speed 也显著下滑时认为正在经历长时间 stall。
  const bool suspected_stall =
      slow_flush && avg_scores.memtable_speed > 0 &&
      current_score_.memtable_speed <
          avg_scores.memtable_speed * 0.5;
  stall_suspect_counter_ =
      suspected_stall ? std::min(stall_suspect_counter_ + 1, kMaxScore)
                      : std::max(0, stall_suspect_counter_ - 1);

  OpType flush_op = kKeep;
  OpType compaction_op = kKeep;
  OpType batch_op = kKeep;
  OpType sstable_op = kKeep;
  if (memtable_pressure_now || memtable_pressure_score_ >= 1 ||
      stall_suspect_counter_ >= 1) {
    flush_op = kLinearIncrease;
    compaction_op = kHalf;
    // batch_op = kLinearIncrease;
  } else if (memtable_relax_counter_ >= kRelaxRounds &&
             current_flush_threads_ > min_thread) {
    flush_op = kHalf;
    // memtable pressure relieved: allow memtable to shrink
    // batch_op = kHalf;
  }

  if (compaction_pressure_score_ >= (severe_compaction ? 1 : 3)) {
    compaction_op = kLinearIncrease;
    // sstable_op = kLinearIncrease;
  } else if (compaction_relax_counter_ >= kRelaxRounds &&
             current_compaction_threads_ > 2) {
    compaction_op = kHalf;
    // backlog cleared: shrink SSTable to avoid runaway growth
    // sstable_op = kHalf;
    // batch_op= kHalf;
  }

  constexpr double kIdleThreshold = 0.5;
  const bool flush_over_provisioned =
      !memtable_pressure_now &&
      current_score_.flush_idle_time > kIdleThreshold &&
      current_flush_threads_ > min_thread;
  if (flush_op == kKeep && flush_over_provisioned) {
    flush_op = kHalf;
  }

  const bool compaction_over_provisioned =
      !compaction_pressure_now &&
      current_score_.compaction_idle_time > kIdleThreshold &&
      current_compaction_threads_ > 2;
  if (compaction_op == kKeep && compaction_over_provisioned) {
    compaction_op = kHalf;
  }

  // Batch/SSTable tuning: decouple from thread decisions (mirrors TEA/FEA).
  if (memtable_pressure_now) {
    batch_op = kLinearIncrease;
  } else if (!memtable_pressure_now && compaction_pressure_now) {
    batch_op = kHalf;
  }

  const bool backlog_relax_ready =
      compaction_relax_counter_ >= kRelaxRounds &&
      !memtable_pressure_now;
  if (compaction_pressure_score_ >= (severe_compaction ? 1 : 2)) {
    sstable_op = kLinearIncrease;
  } else if (backlog_relax_ready) {
    sstable_op = kHalf;
  }

  result.FlushThreadOp = flush_op;
  result.CompactionThreadOp = compaction_op;
  result.BatchOp = batch_op;
  result.SSTableOp = sstable_op;
  std::cout << "[ARK] flush_avg=" << current_score_.flush_speed_avg
            << " baseline=" << flush_baseline
            << " imm=" << current_score_.immutable_number
            << " l0=" << current_score_.l0_num
            << " pending=" << current_score_.estimate_compaction_bytes
            << " mem_score=" << memtable_pressure_score_
            << " comp_score=" << compaction_pressure_score_
            << " stall_cnt=" << stall_suspect_counter_
            << " flush_idle=" << current_score_.flush_idle_time
            << " comp_idle=" << current_score_.compaction_idle_time
            << " ops(flush/comp/batch/sstable)="
            << OpString(result.FlushThreadOp) << "/"
            << OpString(result.CompactionThreadOp) << "/"
            << OpString(result.BatchOp) << "/"
            << OpString(result.SSTableOp) << std::endl;

  return result;
}



void DOTA_Tuner::FillUpChangeListArk(std::vector<ChangePoint> *change_list,
                                     TuningOP op) {
  uint64_t memtable_target = current_opt.write_buffer_size;
  uint64_t sstable_target = current_opt.target_file_size_base;
  const uint64_t original_memtable = memtable_target;
  const uint64_t original_sstable = sstable_target;
  const int original_flush_threads = current_flush_threads_;
  const int original_compaction_threads = current_compaction_threads_;
  bool memtable_changed = false;
  bool sstable_changed = false;
  bool flush_changed = false;
  bool compaction_changed = false;

  // Refresh max thread cap from current options; fall back to core count so ARK
  // can scale up even if initial DB options were conservative.
  int db_thread_cap = current_opt.max_background_jobs > 0 ? current_opt.max_background_jobs
                                                          : core_num;
  if (db_thread_cap > max_thread) {
    max_thread = db_thread_cap;
  }



  // 与 old 版本不同：memtable 和 SSTable 大小 now 独立调节。以前两者强制相等，
  // 现在可以在 MT stall 时保持小 SST，而在 PS stall 时按需放大。
  switch (op.BatchOp) {
    case kLinearIncrease:
      memtable_target += default_opts.write_buffer_size;
      memtable_changed = true;
      break;
    case kHalf:
    //11-22
      memtable_target /= 2;
      // memtable_target -= default_opts.write_buffer_size;
      memtable_changed = true;
      break;
    case kKeep:
      break;
  }
  

  switch (op.SSTableOp) {
    case kLinearIncrease: {
      sstable_target +=  default_opts.target_file_size_base;
      sstable_changed = true;
      break;
    }
    case kHalf:
      sstable_target /= 2;
      // sstable_target -=  default_opts.target_file_size_base;
      sstable_changed = true;
      break;
    case kKeep:
      break;
  }

  auto clamp_size = [](uint64_t value, uint64_t lower, uint64_t upper) {
    return std::min(std::max(value, lower), upper);
  };
  memtable_target =
      clamp_size(memtable_target, min_memtable_size, max_memtable_size);
  sstable_target =
      clamp_size(sstable_target, min_sstable_size, max_sstable_size);
  if (memtable_changed && memtable_target == original_memtable) {
    memtable_changed = false;
  }
  if (sstable_changed && sstable_target == original_sstable) {
    sstable_changed = false;
  }

  OpType flush_op = op.FlushThreadOp;
  OpType compaction_op = op.CompactionThreadOp;
  if (flush_op == kKeep && compaction_op == kKeep && op.ThreadOp != kKeep) {
    flush_op = op.ThreadOp;
    compaction_op = op.ThreadOp;
  }

  // Thread tuning (flush/compaction with shared cap)
  int new_flush_threads = current_flush_threads_;
  switch (flush_op) {
    case kLinearIncrease:
      new_flush_threads += 1;
      flush_changed = true;
      break;
    case kHalf:
      new_flush_threads /= 2;
      // new_flush_threads -= 1;
      flush_changed = true; 
      break;
    case kKeep:
      break;
  }


  int new_compaction_threads = current_compaction_threads_;
  switch (compaction_op) {
    case kLinearIncrease:
      new_compaction_threads += 1;
      compaction_changed = true;
      break;
    case kHalf:
      new_compaction_threads /= 2;
      compaction_changed = true;
      break;
    case kKeep:
      break;
  }

  // Keep flush threads bounded to avoid over-allocation.
  const int flush_thread_cap = 4;
  new_flush_threads = std::max(1, new_flush_threads);
  int flush_upper_bound = std::max(1, max_thread - min_thread);
  int max_allowed_flush_threads = std::min(flush_thread_cap, flush_upper_bound);
  if (new_flush_threads > max_allowed_flush_threads) {
    new_flush_threads = max_allowed_flush_threads;
    if (new_flush_threads != original_flush_threads) {
      flush_changed = true;
    }
  }



  // if (new_flush_threads > current_flush_threads_) {
  //   uint64_t step = default_opts.write_buffer_size > 0
  //                       ? default_opts.write_buffer_size
  //                       : current_opt.write_buffer_size;
  //   if (step == 0) {
  //     step = min_sstable_size;
  //   }
  //   memtable_target += step;
  //   memtable_changed = true;
  // }

  // int compaction_min = flush_target_fixed ? 1 : min_thread;
  new_compaction_threads = std::max(1, new_compaction_threads);
  new_compaction_threads = std::min(new_compaction_threads, max_thread);

  // Expand total cap if needed (but not beyond core_num).
  int target_total = new_flush_threads + new_compaction_threads;
  int total_limit = std::min(core_num, std::max(max_thread, target_total));
  if (total_limit > max_thread) {
    ChangePoint bg_jobs_cp;
    bg_jobs_cp.db_width = true;
    bg_jobs_cp.opt = max_bg_jobs;
    bg_jobs_cp.value = std::to_string(total_limit);
    change_list->push_back(bg_jobs_cp);
    max_thread = total_limit;
  }
  // if (flush_target_fixed) {
  //   // 先满足 flush 目标，再让 compaction 适应剩余线程
  //   while (new_flush_threads + new_compaction_threads > total_limit &&
  //          new_compaction_threads > compaction_min) {
  //     new_compaction_threads--;
  //   }
  //   if (new_flush_threads + new_compaction_threads > total_limit) {
  //     new_flush_threads = std::max(1, total_limit - new_compaction_threads);
  //   } else {
  //     new_flush_threads = new_flush_threads;
  //   }
  // } else {
  //   while (new_flush_threads + new_compaction_threads > total_limit) {
  //     if (new_flush_threads > 1) {
  //       new_flush_threads--;
  //     } else if (new_compaction_threads > compaction_min) {
  //       new_compaction_threads--;
  //     } else {
  //       break;
  //     }
  //   }
  // }

  if (new_flush_threads + new_compaction_threads > total_limit) {
    int excess = new_flush_threads + new_compaction_threads - total_limit;
    new_flush_threads = std::max(1, new_flush_threads - excess);
    new_compaction_threads = std::max(1, new_compaction_threads - excess);
  }
  if (flush_changed && new_flush_threads == original_flush_threads) {
    flush_changed = false;
  }
  if (compaction_changed &&
      new_compaction_threads == original_compaction_threads) {
    compaction_changed = false;
  }

  if (!memtable_changed && !sstable_changed && !flush_changed &&
      !compaction_changed) {
    return;
  }

    // size tuning

  // printf("memtable_target to %lu, sstable size to %lu, flush threads to %d, compaction threads to %d\n",
  //        memtable_target, sstable_target, new_flush_threads,
  //        new_compaction_threads);
  if (memtable_changed || sstable_changed) {
    SetBatchSize(change_list, memtable_target, sstable_target);
  }

  // if (flush_changed){
  //   SetThreadNum(change_list, new_flush_threads, /*for_flush=*/true);
  // }

  // if (compaction_changed){
  //   SetThreadNum(change_list, new_compaction_threads, /*for_flush=*/false);
  // }
  //flush
  if (flush_changed){
    ChangePoint flush_threads_cp;
    flush_threads_cp.db_width = true;
    flush_threads_cp.opt = max_flush_threads_opt;
    flush_threads_cp.value = std::to_string(new_flush_threads);
    change_list->push_back(flush_threads_cp);
    current_flush_threads_ = new_flush_threads;
  }
  // //compaction
  if (compaction_changed){
    ChangePoint comp_threads_cp;
    comp_threads_cp.db_width = true;
    comp_threads_cp.opt = max_compaction_threads_opt;
    comp_threads_cp.value = std::to_string(new_compaction_threads);
    change_list->push_back(comp_threads_cp);
    current_compaction_threads_ = new_compaction_threads;
  }
}



TuningOP FEAT_Tuner::TuneByTEA() {
  // the flushing speed is low.
  TuningOP result{kKeep, kKeep};
 
  if (current_score_.immutable_number >= 1){
    result.ThreadOp = kLinearIncrease;
  }

  if (current_score_.flush_speed_avg < max_scores.flush_speed_avg * TEA_slow_flush && current_score_.flush_speed_avg > 0 ) {
    result.ThreadOp = kHalf;
    std::cout << "slow flush, decrease thread" << std::endl;
  }


  if (current_score_.estimate_compaction_bytes >= 1 || current_score_.l0_num >= 1) {
    result.ThreadOp = kLinearIncrease;
    std::cout << "lo/ro increase, thread" << std::endl;
  }

  std::cout << "[TEA] flush_avg=" << current_score_.flush_speed_avg
            << " max_flush=" << max_scores.flush_speed_avg
            << " imm=" << current_score_.immutable_number
            << " l0=" << current_score_.l0_num
            << " pending=" << current_score_.estimate_compaction_bytes
            << " => op=" << OpString(result.ThreadOp) << std::endl;
  return result;
}

TuningOP FEAT_Tuner::TuneByFEA() {
  TuningOP negative_protocol{kKeep, kKeep};

  if (current_score_.flush_speed_avg <
          max_scores.flush_speed_avg * TEA_slow_flush ||
      current_score_.immutable_number > 1) {
    negative_protocol.BatchOp = kLinearIncrease;
    std::cout << "slow flushing, increase batch" << std::endl;
  }

  if (current_score_.estimate_compaction_bytes >= 1) {
    negative_protocol.BatchOp = kHalf;
    std::cout << "ro, decrease batch" << std::endl;
  }
  return negative_protocol;
}
void FEAT_Tuner::CalculateAvgScore() {
  SystemScores result;
  for (auto score : scores) {
    result = result + score;
  }
  if (scores.size() > 0) result = result / scores.size();
  this->avg_scores = result;
}

}  // namespace ROCKSDB_NAMESPACE

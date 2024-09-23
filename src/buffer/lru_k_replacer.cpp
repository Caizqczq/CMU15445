//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <limits>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex>lock(latch_);

    bool flag=false;
    frame_id_t victim_frame=INVALID_PAGE_ID;
    size_t max_k_dis=std::numeric_limits<size_t>::min();
    //如果没有可以驱逐的帧
    if(curr_size_==0){
        return false;
    }
    for(const auto& it:node_store_){
        if(!it.second.is_evictable_){
            continue;
        }
        //如果历史记录小于k次访问,赋值为+inf
        if(it.second.history_.size()<k_){
            if(!flag||it.second.history_.front()<node_store_.at(victim_frame).history_.front()){
                victim_frame=it.first;
                flag=true;
                max_k_dis=std::numeric_limits<size_t>::max();
            }
        }else{
            //计算访问k次之前的时间戳
            size_t k_dis=current_timestamp_-it.second.history_.back();
            if(k_dis>max_k_dis){
                max_k_dis=k_dis;
                victim_frame=it.first;
                flag=true;
            }
        }  
    }
    if(flag){
        *frame_id=victim_frame;
        //从缓存池中删除对应帧
        node_store_.erase(*frame_id);
        --curr_size_;
        return true;
    }
    return false;
    
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub

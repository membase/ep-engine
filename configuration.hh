/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#ifndef CONFIGURATION_HH
#define CONFIGURATION_HH

#include <string>
#include <map>
#include <assert.h>
#include <iostream>

#include "locks.hh"

#include "memcached/engine.h"


class Configuration {
public:
    Configuration();

    std::string getDbname() const {
        return getString("dbname");
    }

    std::string getShardpattern() const {
        return getString("shardpattern");
    }

    std::string getInitfile() const {
        return getString("initfile");
    }

    std::string getPostInitfile() const {
        return getString("postInitfile");
    }

    std::string getDbStrategy() const {
        return getString("db_strategy");
    }

    bool isWarmup() const {
        return getBool("warmup");
    }

    bool isWaitForWarmup() const {
        return getBool("waitforwarmup");
    }

    bool isFailPartialWarmup() const {
        return getBool("failpartialwarmup");
    }

    bool isVb0() const {
        return getBool("vb0");
    }

    bool isConcurrentDb() const {
        return getBool("concurrentDB");
    }

    size_t getTapKeepalive() const {
        return getInteger("tap_keepalive");
    }

    void setTapKeepalive(size_t nval) {
        setParameter("tap_keepalive", nval);
    }

    size_t getHtSize() const {
        return getInteger("ht_size");
    }

    std::string getStoredValType() const {
        return getString("stored_val_type");
    }

    size_t getHtLocks() const {
        return getInteger("ht_locks");
    }

    size_t getMaxSize() const {
        return getInteger("max_size");
    }

    size_t getMaxTxnSize() const {
        return getInteger("max_txn_size");
    }

    size_t getTapIdleTimeout() const {
        return getInteger("tap_idle_timeout");
    }

    size_t getTapNoopInterval() const {
        return getInteger("tap_noop_interval");
    }

    size_t getMaxItemSize() const {
        return getInteger("max_item_size");
    }

    size_t getMinDataAge() const {
        return getInteger("min_data_age");
    }

    size_t getMemLowWat() const {
        return getInteger("mem_low_wat");
    }

    size_t getMemHighWat() const {
        return getInteger("mem_high_wat");
    }

    size_t getQueueAgeCap() const {
        return getInteger("queue_age_cap");
    }

    size_t getTapBacklogLimit() const {
        return getInteger("tap_backlog_limit");
    }

    size_t getExpiryWindow() const {
        return getInteger("expiry_window");
    }

    size_t getExpPagerStime() const {
        return getInteger("exp_pager_stime");
    }

    size_t getDbShards() const {
        return getInteger("db_shards");
    }

    size_t getMaxVBuckets() const {
        return getInteger("max_vbuckets");
    }

    size_t getVbDelChunkSize() const {
        return getInteger("vb_del_chunk_size");
    }

    size_t getTapBgMaxPending() const {
        return getInteger("tap_bg_max_pending");
    }

    size_t getVbChunkDelTime() const {
        return getInteger("vb_chunk_del_time");
    }

    float getTapBackoffPeriod() const {
        return getFloat("tap_backoff_period");
    }

    size_t getTapAckWindowSize() const {
        return getInteger("tap_ack_window_size");
    }

    size_t getTapAckInterval() const {
        return getInteger("tap_ack_interval");
    }

    size_t getTapAckGracePeriod() const {
        return getInteger("tap_ack_grace_period");
    }

    size_t getTapAckInitialSequenceNumber() const {
        return getInteger("tap_ack_initial_sequence_number");
    }

    size_t getChkRemoverStime() const {
        return getInteger("chk_remover_stime");
    }

    size_t getChkMaxItems() const {
        return getInteger("chk_max_items");
    }

    size_t getChkPeriod() const {
        return getInteger("chk_period");
    }

    bool isInconsistentSlaveChk() const {
        return getBool("inconsistent_slave_chk");
    }

    bool isRestoreMode() const {
        return getBool("restore_mode");
    }

    float getBfResidentThresold() const {
        return getFloat("bf_resident_threshold");
    }

    size_t getGetlDefaultTimeout() const {
        return getInteger("getl_default_timeout");
    }

    size_t getGetlMaxTimeout() const {
        return getInteger("getl_max_timeout");
    }

    size_t getSyncCmdTimeout() const {
        return getInteger("sync_cmd_timeout");
    }

    float getMutationMemThresold() const {
        return getFloat("mutation_mem_threshold");
    }

    std::string getBackend() const {
        return getString("backend");
    }

    std::string getCouchHost() const {
        return getString("couch_host");
    }

    std::string getCouchBucket() const {
        return getString("couch_bucket");
    }

    size_t getCouchPort() const {
        return getInteger("couch_port");
    }

    size_t getCouchReconnectSleeptime() const {
        return getInteger("couch_reconnect_stime");
    }


    bool parseConfiguration(const char *str, SERVER_HANDLE_V1* sapi);

    void addStats(ADD_STAT add_stat, const void *c) const;

    void setParameter(const std::string &key, bool value);
    void setParameter(const std::string &key, size_t value);
    void setParameter(const std::string &key, float value);
    void setParameter(const std::string &key, const char *value);

private:
    std::string getString(const std::string &key) const;
    bool getBool(const std::string &key) const;
    float getFloat(const std::string &key) const;
    size_t getInteger(const std::string &key) const;

    struct value_t {
        config_datatype datatype;
        union {
            size_t v_size;
            float v_float;
            bool v_bool;
            const char *v_string;
        } val;
    };

    // Access to the configuration variables is protected by the mutex
    Mutex mutex;
    std::map<std::string, value_t> attributes;


    friend std::ostream& operator<< (std::ostream& out,
                                     const Configuration &config);
};

#endif

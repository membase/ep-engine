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

#include "config.h"

#include "configuration.hh"
#include <vector>
#include <limits>
#include <sstream>
#include <iostream>

// @todo get the default variables from the correct places (constants)
Configuration::Configuration() {
    setParameter("dbname", "/tmp/test.db");
    setParameter("shardpattern", "%d/%b-%i.sqlite");
    setParameter("db_strategy", "multiDB");
    setParameter("warmup", true);
    setParameter("waitforwarmup", true);
    setParameter("failpartialwarmup", true);
    setParameter("vb0", true);
    setParameter("concurrentDB", true);
    setParameter("tap_keepalive", (size_t)0);
    setParameter("ht_size", (size_t)0);
    setParameter("stored_val_type", (char*)NULL);
    setParameter("ht_locks", (size_t)0);
    setParameter("max_size", (size_t)0);
    setParameter("max_txn_size", (size_t)0);
    setParameter("cache_size", (size_t)0);
    setParameter("tap_idle_timeout", std::numeric_limits<size_t>::max());
    setParameter("tap_noop_interval", (size_t)200); //DEFAULT_TAP_NOOP_INTERVAL;
    setParameter("config_file", (char*)NULL);
    setParameter("max_item_size", (size_t)(20 * 1024 * 1024));
    setParameter("min_data_age", (size_t)0); //DEFAULT_MIN_DATA_AGE;
    setParameter("mem_low_wat", std::numeric_limits<size_t>::max());
    setParameter("mem_high_wat", std::numeric_limits<size_t>::max());
    setParameter("queue_age_cap", (size_t)900); //DEFAULT_QUEUE_AGE_CAP;
    setParameter("tap_backlog_limit", (size_t)5000);
    setParameter("expiry_window", (size_t)3);
    setParameter("exp_pager_stime", (size_t)3600);
    setParameter("db_shards", (size_t)4);
    setParameter("max_vbuckets", (size_t)1024);
    setParameter("vb_del_chunk_size", (size_t)100);
    setParameter("tap_bg_max_pending", (size_t)500);
    setParameter("vb_chunk_del_time", (size_t)500);
    setParameter("tap_backoff_period", (float)5.0);
    setParameter("tap_ack_window_size", (size_t)10);
    setParameter("tap_ack_interval", (size_t)1000);
    setParameter("tap_ack_grace_period", (size_t)300);
    setParameter("tap_ack_initial_sequence_number", (size_t)1);
    setParameter("chk_remover_stime", (size_t)5);
    setParameter("chk_max_items", (size_t)5000);
    setParameter("chk_period", (size_t)600);
    setParameter("inconsistent_slave_chk", false);
    setParameter("restore_mode", false);
    setParameter("bf_resident_threshold", (float)0.9); //DEFAULT_BACKFILL_RESIDENT_THRESHOLD;
    setParameter("getl_default_timeout", (size_t)15);
    setParameter("getl_max_timeout", (size_t)30);
    setParameter("sync_cmd_timeout", (size_t)2500); //DEFAULT_SYNC_TIMEOUT;
    setParameter("mutation_mem_threshold", (float)0.0);
    setParameter("backend", "sqlite");
    setParameter("couch_host", (char*)NULL);
    setParameter("couch_port", (size_t)11213);
    setParameter("couch_reconnect_stime", (size_t)250);
    setParameter("couch_bucket", (char*)"default");

    std::cout.flush();
}

std::string Configuration::getString(const std::string &key) const {
    Mutex *ptr = const_cast<Mutex*> (&mutex);
    LockHolder lh(*ptr);

    std::map<std::string, value_t>::const_iterator iter;
    if ((iter = attributes.find(key)) == attributes.end()) {
        return std::string();
    }
    assert(iter->second.datatype == DT_STRING);
    if (iter->second.val.v_string) {
        return std::string(iter->second.val.v_string);
    }
    return std::string();
}

bool Configuration::getBool(const std::string &key) const {
    Mutex *ptr = const_cast<Mutex*> (&mutex);
    LockHolder lh(*ptr);

    std::map<std::string, value_t>::const_iterator iter;
    if ((iter = attributes.find(key)) == attributes.end()) {
        return false;
    }
    assert(iter->second.datatype == DT_BOOL);
    return iter->second.val.v_bool;
}

float Configuration::getFloat(const std::string &key) const {
    Mutex *ptr = const_cast<Mutex*> (&mutex);
    LockHolder lh(*ptr);

    std::map<std::string, value_t>::const_iterator iter;
    if ((iter = attributes.find(key)) == attributes.end()) {
        return 0;
    }
    assert(iter->second.datatype == DT_FLOAT);
    return iter->second.val.v_float;
}

size_t Configuration::getInteger(const std::string &key) const {
    Mutex *ptr = const_cast<Mutex*> (&mutex);
    LockHolder lh(*ptr);

    std::map<std::string, value_t>::const_iterator iter;
    if ((iter = attributes.find(key)) == attributes.end()) {
        return 0;
    }
    assert(iter->second.datatype == DT_SIZE);
    return iter->second.val.v_size;
}

std::ostream& operator <<(std::ostream &out, const Configuration &config) {
    LockHolder lh(const_cast<Mutex&> (config.mutex));
    std::map<std::string, Configuration::value_t>::const_iterator iter;
    for (iter = config.attributes.begin(); iter != config.attributes.end(); ++iter) {
        std::stringstream line;
        line << iter->first.c_str();
        line << " = [";
        switch (iter->second.datatype) {
        case DT_BOOL:
            if (iter->second.val.v_bool) {
                line << "true";
            } else {
                line << "false";
            }
            break;
        case DT_STRING:
            line << iter->second.val.v_string;
            break;
        case DT_SIZE:
            line << iter->second.val.v_size;
            break;
        case DT_FLOAT:
            line << iter->second.val.v_float;
            break;
        case DT_CONFIGFILE:
            continue;
        default:
            // ignore
            ;
        }
        line << "]" << std::endl;
        out << line.str();
    }

    return out;
}

void Configuration::setParameter(const std::string &key, bool value) {
    LockHolder lh(mutex);
    attributes[key].datatype = DT_BOOL;
    attributes[key].val.v_bool = value;
}

void Configuration::setParameter(const std::string &key, size_t value) {
    LockHolder lh(mutex);
    attributes[key].datatype = DT_SIZE;
    if (key.compare("cache_size") == 0) {
        attributes["max_size"].val.v_size = value;
    } else {
        attributes[key].val.v_size = value;
    }
}

void Configuration::setParameter(const std::string &key, float value) {
    LockHolder lh(mutex);
    attributes[key].datatype = DT_FLOAT;
    attributes[key].val.v_float = value;
}

void Configuration::setParameter(const std::string &key, const char *value) {
    LockHolder lh(mutex);
    if (attributes.find(key) != attributes.end() && attributes[key].datatype
            == DT_STRING) {
        free((void*)attributes[key].val.v_string);
    }
    attributes[key].datatype = DT_STRING;
    attributes[key].val.v_string = NULL;
    if (value != NULL) {
        attributes[key].val.v_string = strdup(value);
    }
}

void Configuration::addStats(ADD_STAT add_stat, const void *c) const {
    LockHolder lh(const_cast<Mutex&> (mutex));
    std::map<std::string, value_t>::const_iterator iter;
    for (iter = attributes.begin(); iter != attributes.end(); ++iter) {
        std::stringstream value;
        switch (iter->second.datatype) {
        case DT_BOOL:
            value << iter->second.val.v_bool;
            break;
        case DT_STRING:
            value << iter->second.val.v_string;
            break;
        case DT_SIZE:
            value << iter->second.val.v_size;
            break;
        case DT_FLOAT:
            value << iter->second.val.v_float;
            break;
        case DT_CONFIGFILE:
        default:
            // ignore
            ;
        }
        add_stat(iter->first.c_str(),
                static_cast<uint16_t> (iter->first.length()),
                value.str().data(),
                static_cast<uint32_t> (value.str().length()), c);
    }
}

class ConfigItem: public config_item {
public:
    ConfigItem(const char *theKey, config_datatype theDatatype) {
        key = theKey;
        datatype = theDatatype;
        value.dt_string = &holder;
    }

private:
    char *holder;
};

bool Configuration::parseConfiguration(const char *str, SERVER_HANDLE_V1* sapi) {

    std::vector<ConfigItem> config;
    config.push_back(ConfigItem("bf_resident_threshold", DT_FLOAT));
    config.push_back(ConfigItem("cache_size", DT_SIZE));
    config.push_back(ConfigItem("chk_max_items", DT_SIZE));
    config.push_back(ConfigItem("chk_period", DT_SIZE));
    config.push_back(ConfigItem("chk_remover_stime", DT_SIZE));
    config.push_back(ConfigItem("concurrentDB", DT_BOOL));
    config.push_back(ConfigItem("config_file", DT_CONFIGFILE));
    config.push_back(ConfigItem("db_shards", DT_SIZE));
    config.push_back(ConfigItem("db_strategy", DT_STRING));
    config.push_back(ConfigItem("dbname", DT_STRING));
    config.push_back(ConfigItem("exp_pager_stime", DT_SIZE));
    config.push_back(ConfigItem("expiry_window", DT_SIZE));
    config.push_back(ConfigItem("failpartialwarmup", DT_BOOL));
    config.push_back(ConfigItem("getl_default_timeout", DT_SIZE));
    config.push_back(ConfigItem("getl_max_timeout", DT_SIZE));
    config.push_back(ConfigItem("ht_locks", DT_SIZE));
    config.push_back(ConfigItem("ht_size", DT_SIZE));
    config.push_back(ConfigItem("inconsistent_slave_chk", DT_BOOL));
    config.push_back(ConfigItem("initfile", DT_STRING));
    config.push_back(ConfigItem("max_item_size", DT_SIZE));
    config.push_back(ConfigItem("max_size", DT_SIZE));
    config.push_back(ConfigItem("max_txn_size", DT_SIZE));
    config.push_back(ConfigItem("max_vbuckets", DT_SIZE));
    config.push_back(ConfigItem("mem_high_wat", DT_SIZE));
    config.push_back(ConfigItem("mem_low_wat", DT_SIZE));
    config.push_back(ConfigItem("min_data_age", DT_SIZE));
    config.push_back(ConfigItem("mutation_mem_threshold", DT_FLOAT));
    config.push_back(ConfigItem("postInitfile", DT_STRING));
    config.push_back(ConfigItem("queue_age_cap", DT_SIZE));
    config.push_back(ConfigItem("restore_mode", DT_BOOL));
    config.push_back(ConfigItem("shardpattern", DT_STRING));
    config.push_back(ConfigItem("stored_val_type", DT_STRING));
    config.push_back(ConfigItem("sync_cmd_timeout", DT_SIZE));
    config.push_back(ConfigItem("tap_ack_grace_period", DT_SIZE));
    config.push_back(ConfigItem("tap_ack_initial_sequence_number", DT_SIZE));
    config.push_back(ConfigItem("tap_ack_interval", DT_SIZE));
    config.push_back(ConfigItem("tap_ack_window_size", DT_SIZE));
    config.push_back(ConfigItem("tap_backlog_limit", DT_SIZE));
    config.push_back(ConfigItem("tap_backoff_period", DT_FLOAT));
    config.push_back(ConfigItem("tap_bg_max_pending", DT_SIZE));
    config.push_back(ConfigItem("tap_idle_timeout", DT_SIZE));
    config.push_back(ConfigItem("tap_keepalive", DT_SIZE));
    config.push_back(ConfigItem("tap_noop_interval", DT_SIZE));
    config.push_back(ConfigItem("vb0", DT_BOOL));
    config.push_back(ConfigItem("vb_chunk_del_time", DT_SIZE));
    config.push_back(ConfigItem("vb_del_chunk_size", DT_SIZE));
    config.push_back(ConfigItem("waitforwarmup", DT_BOOL));
    config.push_back(ConfigItem("warmup", DT_BOOL));

    // @todo I should just add all of the default ones + configfile and all aliases..
    config.push_back(ConfigItem("backend", DT_STRING));
    config.push_back(ConfigItem("couch_port", DT_SIZE));
    config.push_back(ConfigItem("couch_host", DT_STRING));
    config.push_back(ConfigItem("couch_reconnect_stime", DT_SIZE));
    config.push_back(ConfigItem("couch_bucket", DT_STRING));

    struct config_item *items = (struct config_item*)calloc(config.size() + 1,
            sizeof(struct config_item));
    int nelem = config.size();
    for (int ii = 0; ii < nelem; ++ii) {
        items[ii].key = config[ii].key;
        items[ii].datatype = config[ii].datatype;
        items[ii].value.dt_string = config[ii].value.dt_string;
    }

    bool ret = sapi->core->parse_config(str, items, stderr) == 0;
    for (int ii = 0; ii < nelem; ++ii) {
        if (items[ii].found) {
            if (ret) {
                switch (items[ii].datatype) {
                case DT_STRING:
                    setParameter(items[ii].key, *(items[ii].value.dt_string));
                    break;
                case DT_SIZE:
                    setParameter(items[ii].key, *items[ii].value.dt_size);
                    break;
                case DT_BOOL:
                    setParameter(items[ii].key, *items[ii].value.dt_bool);
                    break;
                case DT_FLOAT:
                    setParameter(items[ii].key, *items[ii].value.dt_float);
                    break;
                default:
                    abort();
                }
            }

            if (items[ii].datatype == DT_STRING) {
                free(*items[ii].value.dt_string);
            }
        }
    }

    free(items);
    return ret;

}

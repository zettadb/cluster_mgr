/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef NDEBUG 
#include <list>
#include "cluster_debug.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"

std::list<std::string> g_debug_keys;

namespace kunlun 
{

void ClusterDebug::ParseJson() {
    Json::Value attachment;

    KLOG_INFO("cluster_debug parse input json");
    Json::Value paras = super::get_body_json_document()["paras"];

    std::string keywords, op_type;
    if(paras.isMember("keyword"))
        keywords = paras["keyword"].asString();
    
    if(paras.isMember("op_type")) {
        op_type = paras["op_type"].asString();
    }
    if(op_type.empty())
        return;
    
    std::vector<std::string> key_vec = StringTokenize(keywords, ",");
    if(op_type == "add") {
        for(auto it : key_vec) {
            if(std::find(g_debug_keys.begin(), g_debug_keys.end(), it) == g_debug_keys.end())
                g_debug_keys.emplace_back(it);
        }
    } else if(op_type == "del") {
        for(auto it : key_vec) {
            for(std::list<std::string>::iterator itb = g_debug_keys.begin(), 
                        ite = g_debug_keys.end(); itb != ite; ) {
                if(*itb == it)
                    itb = g_debug_keys.erase(itb);
                else
                    ++itb;
            }
        }
    }
    
    attachment["status"] = "done";
    attachment["error_code"] = "0";
    attachment["error_info"] = "ok";
    set_body_json_attachment(attachment);
}

}

int _cm_keyword(const char* keyword, int state) {
    for(auto it : g_debug_keys) {
        if(strncmp(it.c_str(), keyword, it.length()) == 0)
            return 1;
    }
    return 0;
}
#endif
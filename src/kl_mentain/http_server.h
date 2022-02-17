/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef HTTP_SERVER_H
#define HTTP_SERVER_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"

#include <pthread.h>
#include <vector>
#include <string>
#include <queue>
#include <algorithm>

class Http_server
{
public:
	enum Http_type {HTTP_NONE, HTTP_GET, HTTP_POST};
	enum Content_type {Content_NONE, Application_json, Content_form_urlencoded, Content_form_data};
	static int do_exit;
private:
	static Http_server *m_inst;
	Http_server();
	
	std::vector<pthread_t> vec_pthread;
	pthread_mutex_t thread_mtx;
	pthread_cond_t thread_cond;
	std::queue<int> que_socket;
	int wakeupfd;
	
public:
	~Http_server();
	static Http_server *get_instance()
	{
		if (!m_inst) m_inst = new Http_server();
		return m_inst;
	}
	int start_http_thread();
	void join_all();
	bool Get_http_path(const char* buf, std::string &path);
	bool Get_http_range(const char* buf, uint64_t *begin, uint64_t *end);
	Http_type Get_http_type(const char* buf);
	Content_type Get_http_content_type(const char* buf);
	int Listen_socket(int port);
	void Http_server_handle(int socket);
	void Http_server_handle_get(int &socket, std::string &path);
	void Http_server_handle_get_range(int &socket, std::string &path, uint64_t begin, uint64_t end);
	void Http_server_handle_post_para(int &socket, char* buf, int len);
	void Http_server_handle_post_file(int &socket, char* buf, int len);
	void Http_server_accept();
	void Http_server_work();
};

#endif // !HTTP_SERVER_H

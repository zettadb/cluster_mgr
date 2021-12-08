/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef HTTP_CLIENT_H
#define HTTP_CLIENT_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"

#include <unistd.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <algorithm>

class Http_client
{
public:
	enum Http_type {HTTP_NONE, HTTP_GET, HTTP_POST};
	enum Content_type {Content_NONE, Content_form_urlencoded, Content_form_data};
	static int do_exit;
private:
	static Http_client *m_inst;
	Http_client();
public:
	~Http_client();
	static Http_client *get_instance()
	{
		if (!m_inst) m_inst = new Http_client();
		return m_inst;
	}
	bool Http_client_parse_url(const char *url, char *ip, int *port, char *path);
	bool Http_client_content_length(const char* buf, int *length);
	bool Http_client_get_filename(const char* filepath, std::string &filename_str);
	int Http_client_socket(const char *ip, int port);
	int Http_client_get(const char *url);
	int Http_client_get_file(const char *url, const char *filename, int *pos);
	int Http_client_get_file_range(const char *url, const char *filename, int *pos, int begin, int end);
	int Http_client_post_para(const char *url, const char *post_str, std::string &result_str);
	int Http_client_post_file(const char *url, const char *post_str, const char *filepath, std::string &result_str);
};

#endif // !HTTP_CLIENT_H

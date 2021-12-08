/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "log.h"
#include "sys.h"
#include "job.h"
#include "http_server.h"
#include <signal.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>

#define BUFSIZE 2048		//2K

static const char *lpHttpHtmlOk = "HTTP/1.0 200 OK\r\nContent-Type: text/html\r\nContent-Length: ";
static const char *lpHttpBinOk = "HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ";
static const char *lpHttpJspOk = "HTTP/1.0 200 OK\r\napplication/javascript\r\nContent-Length: ";
static const char *lpHttpError = "HTTP/1.0 400 Bad Request\r\n";
static const char *lpReturnOk = "{\"result\":\"succeed\"}\r\n";

static const char *lpHttpRangeOk = "HTTP/1.0 206 Partial Content\r\n\
Content-Type: text/plain\r\n\
Content-Range: bytes %d-%d/%d\r\n\
Content-Length: %d\r\n\r\n";


extern "C" void *thread_func_http_server_accept(void*thrdarg);
extern "C" void *thread_func_http_server_work(void*thrdarg);

Http_server* Http_server::m_inst = NULL;
int Http_server::do_exit = 0;

int64_t num_http_threads = 3;
int64_t cluster_mgr_http_port = 5000;
int64_t node_mgr_http_port = 5001;
std::string http_web_path;
std::string http_upload_path;

Http_server::Http_server():
	wakeupfd(0)
{

}

Http_server::~Http_server()
{

}

int Http_server::start_http_thread()
{
	int error = 0;
	pthread_mutex_init(&thread_mtx, NULL);
	pthread_cond_init(&thread_cond, NULL);

	//start http server accept thread
	pthread_t hdl;
	if ((error = pthread_create(&hdl,NULL, thread_func_http_server_accept, m_inst)))
	{
		char errmsg_buf[256];
		syslog(Logger::ERROR, "Can not create http server accept thread, error: %d, %s",
					error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
		do_exit = 1;
		return -1;
	}
	vec_pthread.push_back(hdl);

	//start http server work thread
	for(int i=0; i<num_http_threads; i++)
	{
		pthread_t hdl;
		if ((error = pthread_create(&hdl,NULL, thread_func_http_server_work, m_inst)))
		{
			char errmsg_buf[256];
			syslog(Logger::ERROR, "Can not create http server work thread, error: %d, %s",
						error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
			do_exit = 1;
			return -1;
		}
		vec_pthread.push_back(hdl);
	}

	return 0;
}

void Http_server::join_all()
{
	do_exit = 1;
	if(wakeupfd>0)
	{
		uint64_t one = 1;
		write(wakeupfd, &one, sizeof one);	
		close(wakeupfd);
	}
	
	pthread_mutex_lock(&thread_mtx);
	pthread_cond_broadcast(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);

	for (auto &i:vec_pthread)
	{
		pthread_join(i, NULL);
	}
}

bool Http_server::Get_http_path(const char* buf, std::string &path)
{
	char *cStart, *cEnd;

	cStart = strchr((char*)buf, '/');
	if(cStart == NULL)
		return false;
	
	cEnd = strchr(cStart, ' ');
	if(cEnd == NULL)
		return false;
	
	path = std::string(cStart+1, cEnd - cStart - 1);
	//std::transform(path.begin(), path.end(), path.begin(), ::tolower);

	return true;
}

bool Http_server::Get_http_range(const char* buf, uint64_t *begin, uint64_t *end)
{
	char *cStart, *cEnd;
	std::string str;

	cStart = strstr((char*)buf, "Range:");
	if(cStart == NULL)
		return false;

	cStart = strstr(cStart, "bytes=");
	if(cStart == NULL)
		return false;

	cStart = cStart + strlen("bytes=");
	cEnd = strstr(cStart, "-");
	if(cEnd == NULL)
		return false;

	str = std::string(cStart, cEnd - cStart);
	*begin = atol(str.c_str());

	cStart = cEnd+1;
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		return false;
	
	str = std::string(cStart, cEnd - cStart);
	*end = atol(str.c_str());

	return true;
}

Http_server::Http_type Http_server::Get_http_type(const char* buf)
{
	if(strncmp(buf, "GET", 3) == 0)
		return HTTP_GET;
	else if(strncmp(buf, "POST", 3) == 0)
		return HTTP_POST;

	return HTTP_NONE;
}

Http_server::Content_type Http_server::Get_http_content_type(const char* buf)
{
	char *cStart, *cEnd;
	std::string content_type;

	cStart = strstr((char*)buf, "Content-Type:");
	if(cStart == NULL)
		return Content_NONE;

	cStart = cStart + strlen("Content-Type:");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		return Content_NONE;

	content_type = std::string(cStart, cEnd - cStart);

	if(strstr(content_type.c_str(), "application/json") != NULL)
		return Application_json;
	else if(strstr(content_type.c_str(), "x-www-form-urlencoded") != NULL)
		return Content_form_urlencoded;
	else if(strstr(content_type.c_str(), "form-data") != NULL)
		return Content_form_data;

	return Content_NONE;
}

int Http_server::Listen_socket(int port)
{
	int listenfd = socket(AF_INET, SOCK_STREAM|SOCK_CLOEXEC, 0);
	if (listenfd < 0)
	{
		syslog(Logger::ERROR, "socket failed %s", strerror(errno));
		return 0;
	}
	int on = 1;
	if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1)
	{
		syslog(Logger::ERROR, "setsockopt SO_REUSEADDR failed %s", strerror(errno));
		return 0;
	}
	struct timeval timeout = {3,0}; 
	if (setsockopt(listenfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(struct timeval)))
	{
		syslog(Logger::ERROR, "setsockopt SO_RCVTIMEO failed %s", strerror(errno));
		return 0;
	}
	if (setsockopt(listenfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval)))
	{
		syslog(Logger::ERROR, "setsockopt SO_SNDTIMEO failed %s", strerror(errno));
		return 0;
	}
	
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(listenfd, (struct sockaddr *) &addr, sizeof(addr)) == -1)
	{
		syslog(Logger::ERROR, "bind failed %s", strerror(errno));
		return 0;
	}
	if (listen(listenfd, 100) == -1)
	{
		syslog(Logger::ERROR, "listen failed %s", strerror(errno));
		return 0;
	}
	return listenfd;
}

void Http_server::Http_server_handle(int socket)
{
	char http_buf[BUFSIZE];
	memset(http_buf, 0, BUFSIZE);
	int ret = recv(socket, http_buf, BUFSIZE-1, 0);
	if (ret <= 0)
	{
		syslog(Logger::ERROR, "recv failed ret=%d", ret);
	} 
	else
	{
		//syslog(Logger::INFO, "recv:%s", http_buf);
		//std::cout << http_buf;
		Http_type type = Get_http_type(http_buf);
		//syslog(Logger::INFO, "type:%d", type);
		std::string path;
		Get_http_path(http_buf, path);
		//syslog(Logger::INFO, "path:%s", path.c_str());

		if(type == HTTP_GET)
		{
			uint64_t begin, end;
			if(Get_http_range(http_buf, &begin, &end))
				Http_server_handle_get_range(socket, path, begin, end);
			else
				Http_server_handle_get(socket, path);
		}
		else if(type == HTTP_POST)
		{
			Content_type content_type = Get_http_content_type(http_buf);
			//syslog(Logger::INFO, "content_type:%d", content_type);

			if(content_type == Application_json || content_type == Content_form_urlencoded)
				Http_server_handle_post_para(socket, http_buf, ret);
			else if(content_type == Content_form_data)
				Http_server_handle_post_file(socket, http_buf, ret);
			else
				send(socket, lpHttpError, strlen(lpHttpError), 0);
		}
	}

	close(socket);
}

void Http_server::Http_server_handle_get(int &socket, std::string &path)
{
	std::string strpath;
	if(path.length()==0 || path == "hello")
	{
		strpath = http_web_path + "/hello.html";
	}
	else if(path == "para")
	{
		strpath = http_web_path + "/para.html";
	}
	else if(path == "file")
	{
		strpath = http_web_path + "/file.html";
	}
	else
	{
		strpath = http_web_path + "/" + path;
	}

	bool html_file = true;
	if(strpath.find(".html") == size_t(-1))
		html_file = false;

	struct stat buf;
    int res = stat(strpath.c_str(), &buf);
	if(res == 0)
	{
		char http_buf[BUFSIZE];
		uint64_t filelen = buf.st_size;
		uint64_t n;

		if(html_file)
			n = snprintf(http_buf, BUFSIZE, "%s%lu\r\n\r\n", lpHttpHtmlOk, filelen);
		else
			n = snprintf(http_buf, BUFSIZE, "%s%lu\r\n\r\n", lpHttpBinOk, filelen);
		
		send(socket, http_buf, n, 0);

		FILE *fp = fopen(strpath.c_str(), "rb");
		if (fp != NULL)
		{
			while(filelen>0 && !Http_server::do_exit)
			{
				int rlen = fread(http_buf, 1, BUFSIZE, fp);
				int wlen = send(socket, http_buf, rlen, 0);
				int wtotal = wlen;

				while(wlen>=0 && wtotal<rlen && !Http_server::do_exit)
				{
					usleep(1000);
					wlen = send(socket, http_buf+wtotal, rlen-wtotal, 0);
					wtotal += wlen;
				}
				filelen -= wtotal;

				if(wlen<0)
					break;
			}
			fclose(fp);
		}
	}
	else
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
	}
}

void Http_server::Http_server_handle_get_range(int &socket, std::string &path, uint64_t begin, uint64_t end)
{
	bool ret = false;
	std::string strpath;
	if(path.length()==0 || path == "hello")
	{
		strpath = http_web_path + "/hello.html";
	}
	else if(path == "para")
	{
		strpath = http_web_path + "/para.html";
	}
	else if(path == "file")
	{
		strpath = http_web_path + "/file.html";
	}
	else
	{
		strpath = http_web_path + "/" + path;
	}

	struct stat buf;
    int res = stat(strpath.c_str(), &buf);
	if(res == 0)
	{
		char http_buf[BUFSIZE];
		uint64_t len = buf.st_size;
		if(end == 0 || end >= len)
			end = len-1;
		if(begin > len-1 || end < begin)
			goto end;
		
		uint64_t need_to_send = end-begin+1;
		uint64_t n = snprintf(http_buf, BUFSIZE, lpHttpRangeOk, begin, end, len, need_to_send);
		
		send(socket, http_buf, n, 0);

		FILE *fp = fopen(strpath.c_str(), "rb");
		if (fp != NULL)
		{
			fseek(fp, begin, SEEK_SET);
			while(need_to_send>0 && !Http_server::do_exit)
			{
				int rlen;
				int wlen;
				int wtotal;
				
				if(need_to_send>BUFSIZE)
					rlen = fread(http_buf, 1, BUFSIZE, fp);
				else
					rlen = fread(http_buf, 1, need_to_send, fp);
				
				wlen = send(socket, http_buf, rlen, 0);
				wtotal = wlen;

				while(wlen>=0 && wtotal<rlen && !Http_server::do_exit)
				{
					usleep(1000);
					wlen = send(socket, http_buf+wtotal, rlen-wtotal, 0);
					wtotal += wlen;
				}
				need_to_send -= wtotal;

				if(wlen<0)
					break;
			}
			fclose(fp);
		}

		ret = true;
	}

end:
	//syslog(Logger::INFO, "Http_server_handle_get_range ret=%d", ret);
	if(!ret)
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
	}
}

void Http_server::Http_server_handle_post_para(int &socket, char* buf, int len)
{
	char *cStart, *cEnd;
	std::string para, str;
	int content_length;

	////////////////////////////////////////////////////////////////
	cStart = strstr((char*)buf, "Content-Length:");
	if(cStart == NULL)
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
		return;
	}

	cStart = cStart + strlen("Content-Length:");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
		return;
	}

	str = std::string(cStart, cEnd - cStart);
	content_length = atoi(str.c_str());
	if(content_length <= 0)
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
		return;
	}

	//syslog(Logger::INFO, "content_length:%d", content_length);

	////////////////////////////////////////////////////////////////

	cStart = strstr(cStart, "\r\n\r\n");
	if(cStart == NULL)
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
		return;
	}
	cStart = cStart + strlen("\r\n\r\n");
	
	int rlen = len - (cStart-buf);
	//syslog(Logger::INFO, "rlen:%d", rlen);
	if(rlen < content_length)
	{
		para = std::string(cStart, rlen);

		while(rlen < content_length)
		{
			memset(buf, 0, BUFSIZE);
			len = recv(socket, buf, BUFSIZE-1, 0);
			if(len <= 0)
			{
				send(socket, lpHttpError, strlen(lpHttpError), 0);
				return;
			}
			para += std::string(buf, len);
			rlen += len;
		}
	}
	else
	{
		para = std::string(cStart, content_length);
	}

	//add to job
	//syslog(Logger::INFO, "Http_server_handle_post_para:%s", para.c_str());

	//get para type
	std::string str_ret;
	bool ret = Job::get_instance()->http_para_cmd(para, str_ret);
	
	if(ret)
	{
		int n = snprintf(buf, BUFSIZE, "%s%lu\r\n\r\n", lpHttpJspOk, str_ret.length());
		send(socket, buf, n, 0);
		send(socket, str_ret.c_str(), str_ret.length(), 0);
	}
	else
	{
		Job::get_instance()->add_job(para);

		int n = snprintf(buf, BUFSIZE, "%s%lu\r\n\r\n%s", lpHttpJspOk, strlen(lpReturnOk), lpReturnOk);
		send(socket, buf, n, 0);
	}
}

void Http_server::Http_server_handle_post_file(int &socket, char* buf, int len)
{
	char *cStart, *cEnd, *contentStart, *fileStart;
	bool ret = false;
	int content_length,file_length;
	size_t pos1;
	FILE *fp;
	std::string str,filepath,boundary,parameters,filename,db,tb;

	////////////////////////////////////////////////////////////////
	cStart = strstr((char*)buf, "Content-Length:");
	if(cStart == NULL)
		goto end;

	cStart = cStart + strlen("Content-Length:");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		goto end;

	str = std::string(cStart, cEnd - cStart);
	content_length = atoi(str.c_str());
	if(content_length <= 0)
		goto end;

	//syslog(Logger::INFO, "content_length:%d", content_length);

	////////////////////////////////////////////////////////////////
	cStart = strstr((char*)buf, "boundary=");
	if(cStart == NULL)
		goto end;
	
	cStart = cStart + strlen("boundary=");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		goto end;

	boundary = std::string(cStart, cEnd - cStart);

	//syslog(Logger::INFO, "boundary:%s", boundary.c_str());
	
	////////////////////////////////////////////////////////////////
	contentStart = strstr(cEnd, "\r\n\r\n");
	if(contentStart == NULL)
		goto end;

	contentStart = contentStart + strlen("\r\n\r\n");
	if(contentStart-buf == len)
		len += recv(socket, contentStart, BUFSIZE-len-1, 0);

	////////////////////////////////////////////////////////////////
	cStart = strstr(contentStart, "\r\n\r\n");
	if(cStart == NULL)
		goto end;

	cStart = cStart + strlen("\r\n\r\n");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		goto end;

	parameters = std::string(cStart, cEnd - cStart);

	syslog(Logger::INFO, "parameters:%s", parameters.c_str());

	////////////////////////////////////////////////////////////////
	filepath = http_upload_path;

	
	////////////////////////////////////////////////////////////////
	cStart = strstr(cEnd, "filename=\"");
	if(cStart == NULL)
		goto end;
	
	cStart = cStart + strlen("filename=\"");
	cEnd = strstr(cStart, "\"");
	if(cEnd == NULL)
		goto end;

	filename = std::string(cStart, cEnd - cStart);

	//syslog(Logger::INFO, "filename:%s", filename.c_str());

	////////////////////////////////////////////////////////////////
	fileStart = strstr(cEnd, "\r\n\r\n");
	if(fileStart == NULL)
		goto end;

	fileStart = fileStart + strlen("\r\n\r\n");
	if(fileStart-buf == len)
		len += recv(socket, fileStart, BUFSIZE-len-1, 0);

	////////////////////////////////////////////////////////////////
	if(access(filepath.c_str(), F_OK) != 0)
	{
		syslog(Logger::INFO, "need to create path");
		if(mkdir(filepath.c_str(), 0777) == 0)
		{
			syslog(Logger::ERROR, "Creat upload path %s fail!", filepath.c_str());
			//goto end;
		}
	}

	filename = filepath + "/" + filename;
	fp = fopen(filename.c_str(), "wb");
	if(fp == NULL)
	{
		syslog(Logger::ERROR, "Creat upload file %s fail!", filename.c_str());
		goto end;
	}

	file_length = content_length - (fileStart-contentStart) - boundary.length() - 8;
	syslog(Logger::INFO, "file_length:%d", file_length);
	if(len >= (fileStart-buf) + file_length)
	{
		fwrite(fileStart,1,file_length,fp);
		ret = true;
	}
	else
	{
		char http_buf[BUFSIZE];
		int write_length = len-(fileStart-buf);
		fwrite(fileStart,1,write_length,fp);
		file_length -= write_length;

		while(file_length>0)
		{
			write_length = recv(socket, http_buf, sizeof(http_buf), 0);
			if(write_length>=file_length)
			{
				fwrite(http_buf,1,file_length,fp);
				file_length = 0;
				break;
			}
			else if(write_length>0)
			{
				fwrite(http_buf,1,write_length,fp);
				file_length -= write_length;
			}
			else
			{
				break;
			}
		}

		if(file_length == 0)
			ret = true;
	}
	
	fclose(fp);
	
end:
	//syslog(Logger::INFO, "Http_server_handle_post_file ret=%d", ret);
	if(ret)
	{
		int n = snprintf(buf, BUFSIZE, "%s%lu\r\n\r\n%s", lpHttpJspOk, strlen(lpReturnOk), lpReturnOk);
		send(socket, buf, n, 0);
	}
	else
	{
		send(socket, lpHttpError, strlen(lpHttpError), 0);
	}
}

void Http_server::Http_server_accept()
{
	int client_socket = 0;
	struct sockaddr_in client_addr;
	socklen_t len = sizeof(client_addr);
	int listenfd = Listen_socket(cluster_mgr_http_port);
	if(listenfd>0)
		syslog(Logger::INFO, "Http server listen at port: %d start", cluster_mgr_http_port);
	else
		syslog(Logger::ERROR, "Http server listen at port: %d fail", cluster_mgr_http_port);

	wakeupfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

	struct pollfd pfdlisten;
	pfdlisten.fd = listenfd;
	pfdlisten.events = POLLIN;

	struct pollfd pfdwakeup;
	pfdwakeup.fd = wakeupfd;
	pfdwakeup.events = POLLIN;

	std::vector<struct pollfd> pollfds;
	pollfds.push_back(pfdlisten);
	pollfds.push_back(pfdwakeup);

	while (!Http_server::do_exit)
	{
		int nready = poll(&*pollfds.begin(), pollfds.size(), -1);
		if (nready == -1)
		{
			if (errno == EINTR)
				continue;
			
			syslog(Logger::ERROR, "poll failed");
			close(client_socket);
			return;
		}
		else if (nready == 0)	// nothing
		{
			continue;
		}

		if (pollfds[0].revents & POLLIN)
		{
			memset(&client_addr, 0, sizeof(client_addr));
			client_socket = accept4(listenfd, (struct sockaddr *)&client_addr, &len, SOCK_CLOEXEC);
			
			if (client_socket <= 0)
			{
				syslog(Logger::ERROR, "accept failed %d", client_socket);
				close(listenfd);
				return;
			} 
			else
			{
				//syslog(Logger::INFO, "received from %s at port %d", inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
			
				pthread_mutex_lock(&thread_mtx);
				que_socket.push(client_socket);
				pthread_cond_signal(&thread_cond);
				pthread_mutex_unlock(&thread_mtx);
			}

		}

	}
}

void Http_server::Http_server_work()
{
	while (!Http_server::do_exit)  
    {  
		pthread_mutex_lock(&thread_mtx);

        while (que_socket.size() == 0 && !Http_server::do_exit)
            pthread_cond_wait(&thread_cond, &thread_mtx);

		if(Http_server::do_exit)
		{
			pthread_mutex_unlock(&thread_mtx); 
			break;
		}

		int client_socket = que_socket.front();
		que_socket.pop();

		pthread_mutex_unlock(&thread_mtx);

		Http_server_handle(client_socket);
	}
}

extern "C" void *thread_func_http_server_accept(void*thrdarg)
{
	Http_server* httpserver = (Http_server*)thrdarg;
	Assert(httpserver);

	signal(SIGPIPE, SIG_IGN);
	httpserver->Http_server_accept();
	
	return NULL;
}

extern "C" void *thread_func_http_server_work(void*thrdarg)
{
	Http_server* httpserver = (Http_server*)thrdarg;
	Assert(httpserver);

	signal(SIGPIPE, SIG_IGN);
	httpserver->Http_server_work();
	
	return NULL;
}


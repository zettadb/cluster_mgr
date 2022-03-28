/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "log.h"
#include "http_client.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>

#define BUFSIZE 8192		//8K
#define HTTP_DEFAULT_PORT 	80

#define HTTP_GET_STR "GET /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\r\n"

#define HTTP_GET_RANGE_STR "GET /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\
Range: bytes=%s\r\n\r\n"

#define HTTP_POST_PARA_STR "POST /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\
Content-Type: application/json\r\n\
Content-Length: %ld\r\n\r\n\
%s"

#define HTTP_POST_FILE_STR1 "POST /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\
Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Length: %ld\r\n\r\n"

#define HTTP_POST_FILE_STR2 "------WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Disposition: form-data; name=\"parameters\"\r\n\r\n\
%s\r\n\
------WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Disposition: form-data; name=\"filename\"; filename=\"%s\"\r\n\
Content-Type: application/octet-stream\r\n\r\n"

#define HTTP_POST_FILE_STR3 "\r\n------WebKitFormBoundaryZjrentBBjYWJ7gXp--\r\n"

static const char *lpReturnOk = "{\"result\":\"accept\"}";


int Http_client::do_exit = 0;
Http_client* Http_client::m_inst = NULL;

Http_client::Http_client()
{

}

Http_client::~Http_client()
{

}

bool Http_client::Http_client_parse_url(const char *url, char *ip, int *port, char *path)
{
    char *cStart, *cEnd;
    int len = 0;
    if(!url || !ip || !port || !path)
        return false;
 
    cStart = (char *)url;
 
    if(strncmp(cStart, "http://", strlen("http://")) == 0)
        cStart += strlen("http://");
	else
        return false;

	//get ip and path
    cEnd = strchr(cStart, '/');
    if(cEnd != NULL)
	{
        len = cEnd - cStart;
        memcpy(ip, cStart, len);
        ip[len] = '\0';
        if(*(cEnd + 1) != '\0')
			strcpy(path, cEnd+1);
		else
			*path = '\0';
    }
	else
    {
    	strcpy(ip, cStart);
    }
	
    //get port and reset ip
    cStart = strchr(ip, ':');
    if(cStart != NULL)
	{
        *cStart++ = '\0';
        *port = atoi(cStart);
    }
	else
	{
        *port = HTTP_DEFAULT_PORT;
    }
 
    return true;
}

bool Http_client::Http_client_content_length(const char* buf, int *length)
{
	char *cStart, *cEnd;
	bool ret = false;
	std::string str;

	cStart = strstr((char*)buf, "Content-Length:");
	if(cStart == NULL)
		return false;

	cStart = cStart + strlen("Content-Length:");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		return false;

	str = std::string(cStart, cEnd - cStart);
	*length = atoi(str.c_str());
	if(*length <= 0)
		return false;

	return true;
}

bool Http_client::Http_client_get_filename(const char* filepath, std::string &filename_str)
{
	char *cStart, *cEnd;

	cStart = (char*)filepath;
	while(true){
		cEnd = strchr(cStart, '/');
		if(cEnd == NULL)
			break;
		cStart = cEnd+1;
	};

	filename_str = std::string(cStart, strlen(filepath) - (cStart-filepath));
	if(filename_str.length()>0)
		return true;
	else
		return false;
}

int Http_client::Http_client_socket(const char *ip, int port)
{
	struct hostent *hoste;
	struct sockaddr_in server_addr; 
	int socket_fd;

	if((hoste = gethostbyname(ip))==NULL)
		return -1;

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);
	server_addr.sin_addr = *((struct in_addr *)hoste->h_addr);

	if((socket_fd = socket(AF_INET,SOCK_STREAM,0)) == -1)
		return -1;

	struct timeval timeout = {1,0}; 
	if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(struct timeval)))
	{
		syslog(Logger::ERROR, "setsockopt SO_SNDTIMEO failed %s", strerror(errno));
		return -1;
	}

	if(connect(socket_fd, (struct sockaddr *)&server_addr,sizeof(struct sockaddr)) == -1)
		return -1;

	return socket_fd;
}

/*
 * @param
 * url : http get
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : path is to long
 *  -3 : http connect fail
 */
int Http_client::Http_client_get(const char *url)
{
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];

	if(!Http_client_parse_url(url, ip, &port, path))
	{
		syslog(Logger::ERROR, "url fail: %s", url);
		return -1;
	}

	int n = snprintf(http_buf, BUFSIZE, HTTP_GET_STR, path, ip, port);
	if(n >= sizeof(http_buf)-1)
	{
		syslog(Logger::ERROR, "path %s is to long", path);
		return -2;
	}
	
	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		syslog(Logger::ERROR, "http connect fail %s:%d", ip, port);
		return -3;
	}
	
	send(socket_fd, http_buf, n, 0);

	do{
		n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
		if(n>0)
		{
			http_buf[n] = '\0';
			syslog(Logger::INFO, "http get: %s", http_buf);
		}
	}while(n>0);

	close(socket_fd);

	return 0;
}

/*
 * @param
 * url : http get
 * filename : save file
 * pos : save file download pos if error
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : path is to long
 *  -3 : http connect fail
 *  -4 : get content length fail
 *  -5 : find file start fail
 *  -6 : open file fail
 *  -6 : file length download error
 */
int Http_client::Http_client_get_file(const char *url, const char *filename, int *pos)
{
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];
	int len,content_length = 0;
	char *fileStart;
	*pos = 0;

	/////////////////////////////////////////////////////////////
	if(!Http_client_parse_url(url, ip, &port, path))
	{
		syslog(Logger::ERROR, "url fail: %s", url);
		return -1;
	}

	/////////////////////////////////////////////////////////////
	int n = snprintf(http_buf, BUFSIZE, HTTP_GET_STR, path, ip, port);
	if(n >= sizeof(http_buf)-1)
	{
		syslog(Logger::ERROR, "path %s is to long", path);
		return -2;
	}

	/////////////////////////////////////////////////////////////
	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		syslog(Logger::ERROR, "http connect fail %s:%d", ip, port);
		return -3;
	}
	
	send(socket_fd, http_buf, n, 0);

	/////////////////////////////////////////////////////////////
	len = recv(socket_fd, http_buf, BUFSIZE, 0);
	if(!Http_client_content_length(http_buf, &content_length))
	{
		syslog(Logger::ERROR, "get content length fail");
		close(socket_fd);
		return -4;
	}

	/////////////////////////////////////////////////////////////
	fileStart = strstr(http_buf, "\r\n\r\n");
	if(fileStart == NULL)
	{
		syslog(Logger::ERROR, "find file start fail");
		close(socket_fd);
		return -5;
	}
	fileStart = fileStart + strlen("\r\n\r\n");
	if(fileStart-http_buf == len)
	{
		fileStart = http_buf;
		len = recv(socket_fd, fileStart, BUFSIZE, 0);
	}
	else
	{
		len = len-(fileStart-http_buf);
	}

	/////////////////////////////////////////////////////////////
	FILE *fp = fopen(filename, "wb");
	if (fp == NULL)
	{
		syslog(Logger::ERROR, "open file %s error", filename);
		close(socket_fd);
		return -6;
	}

	while(len > 0)
	{
		fwrite(fileStart,1,len,fp);
		*pos += len;
		fileStart = http_buf;
		len = recv(socket_fd, fileStart, BUFSIZE, 0);
	}

	fclose(fp);
	close(socket_fd);

	/////////////////////////////////////////////////////////////
	if(content_length != *pos)
	{
		syslog(Logger::ERROR, "file length download %d error", *pos);
		return -7;
	}

	return 0;
}

/*
 * @param
 * url : http get
 * filename : save file
 * pos : save file download pos if error
 * begin : the begin of download range
 * end : the end of download range, 0 as default end of the file
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : path is to long
 *  -3 : http connect fail
 *  -4 : get content length fail
 *  -5 : find file start fail
 *  -6 : open file fail
 *  -6 : file length download error
 */
int Http_client::Http_client_get_file_range(const char *url, const char *filename, int *pos, int begin, int end)
{
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];
	int len,content_length = 0;
	char *fileStart;
	*pos = begin;

	/////////////////////////////////////////////////////////////
	if(!Http_client_parse_url(url, ip, &port, path))
	{
		syslog(Logger::ERROR, "url fail: %s", url);
		return -1;
	}

	/////////////////////////////////////////////////////////////
	std::string str = std::to_string(begin) + "-";
	if(end > 0)
		str += std::to_string(end);
	int n = snprintf(http_buf, BUFSIZE, HTTP_GET_RANGE_STR, path, ip, port, str.c_str());
	if(n >= sizeof(http_buf)-1)
	{
		syslog(Logger::ERROR, "path %s is to long", path);
		return -2;
	}

	/////////////////////////////////////////////////////////////
	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		syslog(Logger::ERROR, "http connect fail %s:%d", ip, port);
		return -3;
	}

	send(socket_fd, http_buf, n, 0);

	/////////////////////////////////////////////////////////////
	len = recv(socket_fd, http_buf, BUFSIZE, 0);
	if(!Http_client_content_length(http_buf, &content_length))
	{
		syslog(Logger::ERROR, "get content length fail");
		close(socket_fd);
		return -4;
	}

	/////////////////////////////////////////////////////////////
	fileStart = strstr(http_buf, "\r\n\r\n");
	if(fileStart == NULL)
	{
		syslog(Logger::ERROR, "find file start fail");
		close(socket_fd);
		return -5;
	}
	fileStart = fileStart + strlen("\r\n\r\n");
	if(fileStart-http_buf == len)
	{
		fileStart = http_buf;
		len = recv(socket_fd, fileStart, BUFSIZE, 0);
	}
	else
	{
		len = len-(fileStart-http_buf);
	}

	/////////////////////////////////////////////////////////////
	FILE *fp = fopen(filename, "ab");
	if (fp == NULL)
	{
		syslog(Logger::ERROR, "open file %s error", filename);
		close(socket_fd);
		return -6;
	}

	while(len > 0)
	{
		fwrite(fileStart,1,len,fp);
		*pos += len;
		fileStart = http_buf;
		len = recv(socket_fd, fileStart, BUFSIZE, 0);
	}

	fclose(fp);
	close(socket_fd);

	/////////////////////////////////////////////////////////////
	if(content_length != *pos)
	{
		syslog(Logger::ERROR, "file length download %d error", *pos);
		return -7;
	}

	return 0;
}

/*
 * @param
 * url : http post
 * post_str : post parameter
 * result_str : return parameter
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : post_str is to long
 *  -3 : http connect fail
 *  -4 : http return error
 */
int Http_client::Http_client_post_para(const char *url, const char *post_str, std::string &result_str)
{
	//syslog(Logger::INFO, "Http_client_post_para: %s,%s", url, post_str);
	
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];
	int ret = 0;

	if(!Http_client_parse_url(url, ip, &port, path))
	{
		syslog(Logger::ERROR, "url fail: %s", url);
		return -1;
	}

	int n = snprintf(http_buf, BUFSIZE, HTTP_POST_PARA_STR, path, ip, port, strlen(post_str), post_str);
	if(n >= sizeof(http_buf)-1)
	{
		syslog(Logger::ERROR, "post_str %s is to long", post_str);
		return -2;
	}
	
	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		syslog(Logger::ERROR, "http connect fail %s:%d", ip, port);
		return -3;
	}
	
	send(socket_fd, http_buf, n, 0);

	n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
	if(n>0)
	{
		http_buf[n] = '\0';
		if(strstr((char*)http_buf, "200 OK") == NULL)
		{
			syslog(Logger::ERROR, "http return code error");
			ret = -4;
		}
		else
		{
			int content_length = 0;
			if(!Http_client_content_length(http_buf, &content_length))
			{
				syslog(Logger::ERROR, "get content length fail");
				ret = -4;
			}
			else
			{
				char *result_start = strstr((char*)http_buf, "\r\n\r\n");
				if(result_start == NULL)
					ret = -4;
				else
				{
					int len = n-(result_start+4-http_buf);
					result_str = std::string(result_start+4, len);
					while(len < content_length)
					{
						n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
						len += n;
						result_str += std::string(http_buf, n);
					}
				}
			}
		}
	}

	close(socket_fd);

	return ret;
}

/*
 * @param
 * url : http post
 * post_str : post parameter
 * filepath : filepath for upload 
 * result_str : return parameter
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : post_str is to long
 *  -3 : http connect fail
 *  -4 : upload file no find
 *  -5 : upload file open fail
 *  -6 : http return error
 */
int Http_client::Http_client_post_file(const char *url, const char *post_str, const char *filepath, std::string &result_str)
{
	//syslog(Logger::INFO, "Http_client_post_file: %s,%s,%s,%s", url, post_str, filename, filepath);
	
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];
	int ret = 0;

	uint64_t contentlen = 0;
	uint64_t filelen = 0;

	//////////////////////////////////////////////////////////////////////
	//get file len
	struct stat buf;
    int res = stat(filepath, &buf);
	if(res == 0)
	{
		filelen = buf.st_size;
		syslog(Logger::INFO, "file len = %ld", filelen);
	}
	else
	{
		syslog(Logger::ERROR, "file no find!");
		return -4;
	}

	//////////////////////////////////////////////////////////////////////
	std::string filename;
	Http_client_get_filename(filepath, filename);
	
	//////////////////////////////////////////////////////////////////////
	//get content len
	contentlen = snprintf(http_buf, BUFSIZE, HTTP_POST_FILE_STR2, post_str, filename.c_str());
	contentlen += filelen;
	contentlen += strlen(HTTP_POST_FILE_STR3);
	syslog(Logger::INFO, "contentlen = %ld", contentlen);

	//////////////////////////////////////////////////////////////////////
	if(!Http_client_parse_url(url, ip, &port, path))
	{
		syslog(Logger::ERROR, "url fail: %s", url);
		return -1;
	}
	
	int n = snprintf(http_buf, sizeof(http_buf), HTTP_POST_FILE_STR1, path, ip, port, contentlen);
	n += snprintf(http_buf+n, sizeof(http_buf), HTTP_POST_FILE_STR2, post_str, filename.c_str());
	if(n >= sizeof(http_buf)-1)
	{
		syslog(Logger::ERROR, "post_str %s is to long", post_str);
		close(socket_fd);
		return -2;
	}

	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		syslog(Logger::ERROR, "http connect fail %s:%d", ip, port);
		return -3;
	}

	send(socket_fd, http_buf, n, 0);

	FILE *fp = fopen(filepath, "rb");
	if (fp == NULL)
	{
		syslog(Logger::ERROR, "open file %s error", filepath);
		close(socket_fd);
		return -5;
	}
	
	while(filelen>0)
	{
		int rlen = fread(http_buf, 1, BUFSIZE, fp);
		int wlen = send(socket_fd, http_buf, rlen, 0);
		int wtotal = wlen;
		
		while(wlen>=0 && wtotal<rlen)
		{
			usleep(1000);
			wlen = send(socket_fd, http_buf+wtotal, rlen-wtotal, 0);
			wtotal += wlen;
		}
		filelen -= wtotal;

		if(wlen<0)
			break;
	}
	
	fclose(fp);

	send(socket_fd, HTTP_POST_FILE_STR3, strlen(HTTP_POST_FILE_STR3), 0);

	n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
	if(n>0)
	{
		http_buf[n] = '\0';
		if(strstr((char*)http_buf, "200 OK") == NULL)
		{
			syslog(Logger::ERROR, "http return code error");
			ret = -6;
		}
		else
		{
			int content_length = 0;
			if(!Http_client_content_length(http_buf, &content_length))
			{
				syslog(Logger::ERROR, "get content length fail");
				ret = -6;
			}
			else
			{
				char *result_start = strstr((char*)http_buf, "\r\n\r\n");
				if(result_start == NULL)
					ret = -6;
				else
				{
					int len = n-(result_start+4-http_buf);
					result_str = std::string(result_start+4, len);
					while(len < content_length)
					{
						n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
						len += n;
						result_str += std::string(http_buf, n);
					}
				}
			}
		}
	}

	close(socket_fd);

	return ret;
}


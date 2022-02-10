#  Copyright (c) 2019 ZettaDB inc. All rights reserved.
#  This source code is licensed under Apache 2.0 License,
#  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(cluster_mgr VERSION 1.0)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Werror -O2")
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -O2")
endif()

link_directories(${CMAKE_SOURCE_DIR}/vendor/lib)
configure_file(sys_config.h.in sys_config.h)
add_subdirectory(http_server)
add_subdirectory(request_framework)
add_subdirectory(op_mysql)
add_subdirectory(util_func)
add_subdirectory(cluster_expand)
add_executable(cluster_mgr
config.cc log.cc main.cc os.cc shard.cc sys.cc txn.cc thread_manager.cc kl_cluster.cc machine_info.cc
http_server.cc http_client.cc job.cc cjson.cc)
target_include_directories(cluster_mgr PUBLIC
		"${PROJECT_BINARY_DIR}"
		"${PROJECT_SOURCE_DIR}/vendor/include"
		"${PROJECT_SOURCE_DIR}/http_server"
		"${PROJECT_SOURCE_DIR}/request_framework"
		"${PROJECT_SOURCE_DIR}/op_mysql"
		"${PROJECT_SOURCE_DIR}/util_func"
    "${PROJECT_SOURCE_DIR}/cluster_expand")
target_link_libraries(cluster_mgr http_server request_framework op_mysql util_func cluster_expand libjsoncpp.a libbrpc.a libgflags.a libprotobuf.a libleveldb.a mariadb pthread pq libssl.a libcrypto.a dl z)

install(TARGETS cluster_mgr DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/resources/script/start_cluster_mgr.sh DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/resources/script/stop_cluster_mgr.sh DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/resources/script/restart_cluster_mgr.sh DESTINATION bin)
install(FILES ../README.md ../LICENSE ../NOTICE DESTINATION .)
install(FILES ./cluster_mgr_safe DESTINATION bin)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()


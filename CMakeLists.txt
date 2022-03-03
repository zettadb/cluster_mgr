#  Copyright (c) 2019 ZettaDB inc. All rights reserved.
#  This source code is licensed under Apache 2.0 License,
#  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(cluster_mgr VERSION 1.0)
set(CMAKE_BUILD_TYPE "Debug")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Werror -O2")
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -O2")
endif()
set(ZETTALIB_INCLUDE_PATH "/home/snowao/code/kunlun/zettalib/build/output/include")
set(ZETTALIB_LIB_PATH "/home/snowao/code/kunlun/zettalib/build/output/lib")

link_directories(${PROJECT_SOURCE_DIR}/src/vendor/lib)
link_directories(${ZETTALIB_LIB_PATH})
configure_file(src/sys_config.h.in sys_config.h)
add_subdirectory(src/kl_mentain)
add_subdirectory(src/http_server)
add_subdirectory(src/request_framework)
add_subdirectory(src/util_func)
add_subdirectory(src/cluster_expand)
add_executable(cluster_mgr src/main.cc)
target_include_directories(cluster_mgr PUBLIC
  "${PROJECT_BINARY_DIR}"
  "${ZETTALIB_INCLUDE_PATH}"
  "${PROJECT_SOURCE_DIR}/src/vendor/include"
  "${PROJECT_SOURCE_DIR}/src/kl_mentain"
  "${PROJECT_SOURCE_DIR}/src/http_server"
  "${PROJECT_SOURCE_DIR}/src/request_framework"
  "${PROJECT_SOURCE_DIR}/src/util_func"
  "${PROJECT_SOURCE_DIR}/src/cluster_expand")
target_link_libraries(cluster_mgr 
  kl_mentain
  http_server 
  request_framework 
  util_func 
  cluster_expand 
  libzettalib.a
  libjsoncpp.a 
  libbrpc.a 
  libgflags.a 
  libprotobuf.a 
  libleveldb.a 
  mariadb 
  pthread 
  pq 
  tcmalloc
  libssl.a 
  libcrypto.a 
  dl 
  z)

set(CMAKE_INSTALL_PREFIX "${PROJECT_BINARY_DIR}/output")
install(TARGETS cluster_mgr DESTINATION bin)
install(FILES 
  ${CMAKE_SOURCE_DIR}/resource/script/start_cluster_mgr.sh
  ${CMAKE_SOURCE_DIR}/resource/script/stop_cluster_mgr.sh 
  ${CMAKE_SOURCE_DIR}/resource/script/restart_cluster_mgr.sh 
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/resource/cluster_mgr.cnf DESTINATION conf)
install(DIRECTORY DESTINATION log)
install(DIRECTORY DESTINATION data)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()


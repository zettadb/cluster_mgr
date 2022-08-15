#  Copyright (c) 2019 ZettaDB inc. All rights reserved.
#  This source code is licensed under Apache 2.0 License,
#  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(cluster_mgr VERSION 1.0)
#set(CMAKE_BUILD_TYPE "Debug")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Werror -O2")
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -O2")
endif()

#set(ZETTALIB_PATH "/home/snowao/code/kunlun/zettalib")
if(NOT DEFINED ZETTALIB_PATH)
  message(FATAL_ERROR "ZETTALIB_PATH variable is not speicified, set by -DZETTALIB_PATH=?")
endif()

set(ZETTALIB_INCLUDE_PATH "${ZETTALIB_PATH}/build/output/include")
set(ZETTALIB_LIB_PATH "${ZETTALIB_PATH}/build/output/lib")
set(VENDOR_OUTPUT_PATH "${ZETTALIB_PATH}/src/vendor/vendor_output")

add_definitions("-Wno-error=format-security")
add_definitions(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)
link_directories(${VENDOR_OUTPUT_PATH}/lib)
link_directories(${ZETTALIB_LIB_PATH})
configure_file(src/sys_config.h.in sys_config.h)
add_subdirectory(src/kl_mentain)
add_subdirectory(src/http_server)
add_subdirectory(src/request_framework)
add_subdirectory(src/util_func)
add_subdirectory(src/cluster_expand)
add_subdirectory(src/raft_ha)
add_subdirectory(src/example_mission)
add_subdirectory(src/sync_mission)
add_subdirectory(src/backup_storage)
add_subdirectory(src/cluster_mission)
add_subdirectory(src/other_mission)
add_subdirectory(src/cluster_debug)
add_subdirectory(src/rebuild_node)
add_subdirectory(src/coldbackup)
add_subdirectory(src/cluster_collection)
add_subdirectory(src/cluster_operator)
add_subdirectory(src/restore_cluster)
add_subdirectory(src/cluster_auto)
add_executable(cluster_mgr src/main.cc)

target_include_directories(cluster_mgr PUBLIC
  "${PROJECT_BINARY_DIR}"
  "${ZETTALIB_INCLUDE_PATH}"
  "${VENDOR_OUTPUT_PATH}/include"
  "${PROJECT_SOURCE_DIR}/src/kl_mentain"
  "${PROJECT_SOURCE_DIR}/src/http_server"
  "${PROJECT_SOURCE_DIR}/src/request_framework"
  "${PROJECT_SOURCE_DIR}/src/util_func"
  "${PROJECT_SOURCE_DIR}/src/cluster_expand"
  "${PROJECT_SOURCE_DIR}/src/raft_ha"
  "${PROJECT_SOURCE_DIR}/src/sync_mission"
  "${PROJECT_SOURCE_DIR}/src/backup_storage"
  "${PROJECT_SOURCE_DIR}/src/cluster_mission"
  "${PROJECT_SOURCE_DIR}/src/other_mission"
  "${PROJECT_SOURCE_DIR}/src/rebuild_node"
  "${PROJECT_SOURCE_DIR}/src/cluster_collection"
  "${PROJECT_SOURCE_DIR}/src/coldbackup"
  "${PROJECT_SOURCE_DIR}/src/cluster_debug"
  "${PROJECT_SOURCE_DIR}/src/cluster_operator"
  "${PROJECT_SOURCE_DIR}/src/restore_cluster"
  "${PROJECT_SOURCE_DIR}/src/cluster_auto"
  "${PROJECT_SOURCE_DIR}/src/example_mission")
target_link_libraries(cluster_mgr 
  kl_mentain
  http_server 
  request_framework 
  util_func 
  cluster_auto
  cluster_expand 
  cluster_debug
  raft_ha
  example_mission
  cluster_collection
  sync_mission
  backup_storage
  cluster_mission
  other_mission
  rebuild_node
  coldbackup
  cluster_operator
  restore_cluster
  libzettalib.a
  libbraft.a
  libbrpc.a 
  libjsoncpp.a 
  libspdlog.a
  libgflags.a 
  libprotobuf.a 
  libleveldb.a 
  libmariadb.a
  libpq.a 
  libssl.a 
  libcrypto.a 
  pthread 
  dl 
  z)

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "${PROJECT_BINARY_DIR}/output" CACHE PATH "..." FORCE)
endif()
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
install(DIRECTORY DESTINATION data/cluster_mgr_raft)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()


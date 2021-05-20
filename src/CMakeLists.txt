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
link_directories(${CMAKE_SOURCE_DIR}/../lib)
add_executable(cluster_mgr config.cc  log.cc  main.cc  os.cc  shard.cc  sys.cc  txn.cc thread_manager.cc)
configure_file(sys_config.h.in sys_config.h)
target_include_directories(cluster_mgr PUBLIC
		"${PROJECT_BINARY_DIR}"
		"${PROJECT_SOURCE_DIR}/../include")
target_link_libraries(cluster_mgr mariadb pthread)

install(TARGETS cluster_mgr DESTINATION bin)
install(DIRECTORY ../lib/ DESTINATION lib)
install(DIRECTORY ../resources/ DESTINATION resources)
install(FILES ../README.md ../LICENSE ../NOTICE DESTINATION .)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()


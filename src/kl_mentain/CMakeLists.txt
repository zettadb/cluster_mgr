add_library(kl_mentain OBJECT 
  config.cc 
  log.cc 
  os.cc 
  shard.cc 
  sys.cc 
  txn.cc 
  thread_manager.cc 
  kl_cluster.cc 
  machine_info.cc
  http_server.cc 
  http_client.cc 
  job.cc 
  cjson.cc)
target_include_directories(kl_mentain INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(kl_mentain PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(kl_mentain PUBLIC "${PROJECT_SOURCE_DIR}/src/vendor/include")
target_include_directories(kl_mentain PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(kl_mentain PUBLIC "${ZETTALIB_INCLUDE_PATH}")

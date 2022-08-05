add_library(kl_mentain OBJECT 
  config.cc 
  os.cc 
  shard.cc 
  sys.cc 
  txn.cc 
  kl_cluster.cc 
  async_mysql.cc
  func_timer.cc
)
target_include_directories(kl_mentain INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(kl_mentain PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(kl_mentain PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(kl_mentain PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(kl_mentain PUBLIC "${ZETTALIB_INCLUDE_PATH}")

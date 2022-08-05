add_library(cluster_auto OBJECT 
    refresh_shard.cc ) 
target_include_directories(cluster_auto INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(cluster_auto PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(cluster_auto PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(cluster_auto PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(cluster_auto PUBLIC "${ZETTALIB_INCLUDE_PATH}")
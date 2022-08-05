add_library(cluster_collection OBJECT 
    machine_alive.cc
    prometheus_control.cc)
target_include_directories(cluster_collection INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(cluster_collection PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(cluster_collection PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(cluster_collection PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(cluster_collection PUBLIC "${ZETTALIB_INCLUDE_PATH}")
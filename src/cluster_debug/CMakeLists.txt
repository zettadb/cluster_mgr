add_library(cluster_debug OBJECT 
    cluster_debug.cc)
target_include_directories(cluster_debug INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(cluster_debug PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(cluster_debug PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(cluster_debug PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(cluster_debug PUBLIC "${ZETTALIB_INCLUDE_PATH}")
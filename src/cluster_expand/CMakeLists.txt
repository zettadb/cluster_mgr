add_library(cluster_expand OBJECT expand_mission.cc table_pick.cc)
target_include_directories(cluster_expand INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(cluster_expand PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(cluster_expand PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(cluster_expand PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(cluster_expand PUBLIC "${ZETTALIB_INCLUDE_PATH}")

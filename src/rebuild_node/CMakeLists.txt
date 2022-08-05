add_library(rebuild_node OBJECT 
    rebuild_node.cc)
target_include_directories(rebuild_node INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(rebuild_node PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(rebuild_node PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(rebuild_node PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(rebuild_node PUBLIC "${ZETTALIB_INCLUDE_PATH}")
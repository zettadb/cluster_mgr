add_library(cluster_mission OBJECT 
    machine_mission.cc
    cluster_comm.cc
    create_cluster.cc
    delete_cluster.cc
    add_computer.cc
    add_shard.cc
    delete_computer.cc
    delete_shard.cc
    add_node.cc
    delete_node.cc)
target_include_directories(cluster_mission INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(cluster_mission PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(cluster_mission PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(cluster_mission PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(cluster_mission PUBLIC "${ZETTALIB_INCLUDE_PATH}")

add_library(raft_ha OBJECT 
    raft_ha.cc
    raft_mission.cc
    proto/raft_msg.pb.cc 
    generate_id.cc)
target_include_directories(raft_ha INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(raft_ha PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(raft_ha PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(raft_ha PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(raft_ha PUBLIC "${ZETTALIB_INCLUDE_PATH}")

add_library(request_framework OBJECT 
    handleRequestThread.cc 
    missionRequest.cc 
    requestBase.cc 
    remoteTask.cc)
target_include_directories(request_framework INTERFACE "${PROJECT_SOURCE_DIR}")
target_include_directories(request_framework PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(request_framework PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(request_framework PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(request_framework PUBLIC "${ZETTALIB_INCLUDE_PATH}")

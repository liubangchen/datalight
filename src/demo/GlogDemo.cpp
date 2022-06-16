
#include <iostream>
#include <glog/logging.h>

int main(int argc, char *argv[]){
    // 初始化日志库
    google::InitGoogleLogging(argv[0]);
    LOG(ERROR) << "Hello, World!";
    return 0;
}

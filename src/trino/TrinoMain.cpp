
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <server/TrinoServer.h>
//#include "velox/common/base/StatsReporter.h"

int main(int argc, char * argv[])
{
    folly::init(&argc, &argv);
    datalight::server::TrinoServer server;
    server.run();
}

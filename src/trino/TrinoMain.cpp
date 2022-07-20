#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <server/TrinoServer.h>
#include <hash-library/sha256.h>
#include "velox/common/base/StatsReporter.h"

DEFINE_string(etc_dir, ".", "etc directory for presto configuration");

int main(int argc, char * argv[])
{
    folly::init(&argc, &argv);
    datalight::server::TrinoServer server(FLAGS_etc_dir);
    server.run();
    LOG(INFO) << "SHUTDOWN: Exiting main()";
}


// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
    return new facebook::velox::DummyStatsReporter();
});

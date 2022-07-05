
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <server/TrinoServer.h>
#include <hash-library/sha256.h>

int main(int argc, char * argv[])
{
    folly::init(&argc, &argv);
    datalight::server::TrinoServer server;
    hashlibrary::SHA256 sha256;
    std::cout << sha256("production") << std::endl;
    server.run();
}

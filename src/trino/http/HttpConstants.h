#pragma once

namespace datalight::http
{

    const int kHttpOk = 200;
    const int kHttpAccepted = 202;
    const int kHttpNoContent = 204;
    const int kHttpNotFound = 404;
    const int kHttpInternalServerError = 500;

    const char kMimeTypeApplicationJson[] = "application/json";
    const char kMimeTypeApplicationThrift[] = "application/x-thrift+binary";

}

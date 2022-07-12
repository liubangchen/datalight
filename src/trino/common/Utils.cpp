#include "Utils.h"
#include <fmt/format.h>

namespace datalight::util
{

    protocol::DateTime toISOTimestamp(uint64_t timeMilli)
    {
        char buf[80];
        time_t timeSecond = timeMilli / 1000;
        tm gmtTime;
        gmtime_r(&timeSecond, &gmtTime);
        strftime(buf, sizeof buf, "%FT%T", &gmtTime);
        return fmt::format("{}.{:03d}Z", buf, timeMilli % 1000);
    }

}

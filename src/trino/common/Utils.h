#pragma once


#include "protocol/TrinoProtocol.h"

namespace datalight::util
{
    protocol::DateTime toISOTimestamp(uint64_t timeMilli);
}

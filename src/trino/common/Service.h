#pragma once

namespace datalight::common
{
    class Service
    {
    public:
        virtual void init() = 0;
        virtual int start() const = 0;
        virtual void stop() = 0;
    };
}

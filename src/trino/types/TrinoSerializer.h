#pragma once

#include "velox/common/base/Crc.h"
#include "velox/vector/VectorStream.h"

using namespace facebook::velox;

namespace datalight::trino {

    class TrinoVectorSerde : public VectorSerde {
    public:
        struct PrestoOptions : VectorSerde::Options {
            explicit PrestoOptions(bool useLosslessTimestamp)
                : useLosslessTimestamp(useLosslessTimestamp) {}
            bool useLosslessTimestamp{false};
        };

        void estimateSerializedSize(
            std::shared_ptr<BaseVector> vector,
            const folly::Range<const IndexRange*>& ranges,
            vector_size_t** sizes) override;

        std::unique_ptr<VectorSerializer> createSerializer(
            std::shared_ptr<const RowType> type,
            int32_t numRows,
            StreamArena* streamArena,
            const Options* options) override;

        void deserialize(
            ByteStream* source,
            memory::MemoryPool* pool,
            std::shared_ptr<const RowType> type,
            std::shared_ptr<RowVector>* result,
            const Options* options) override;

        static void registerVectorSerde();
    };


    class TrinoOutputStreamListener : public OutputStreamListener {
    public:
        void onWrite(const char* s, std::streamsize count) override {
            if (not paused_) {
                crc_.process_bytes(s, count);
            }
        }

        void pause() {
            paused_ = true;
        }

        void resume() {
            paused_ = false;
        }

        auto crc() const {
            return crc_;
        }

        void reset() {
            crc_.reset();
        }

    private:
        bool paused_{false};
        bits::Crc32 crc_;
    };
}

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "Base64Util.h"
#include "velox/common/base/BitUtil.h"
#include "velox/common/encode/Base64.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace datalight::protocol {
    namespace {

        static const char* kLongArray = "LONG_ARRAY";
        static const char* kIntArray = "INT_ARRAY";
        static const char* kShortArray = "SHORT_ARRAY";
        static const char* kByteArray = "BYTE_ARRAY";
        static const char* kVariableWidth = "VARIABLE_WIDTH";
        static const char* kRle = "RLE";
        static const char* kArray = "ARRAY";

        struct ByteStream {
            explicit ByteStream(const char* data, int32_t offset = 0)
                : data_(data), offset_(offset) {}

            template <typename T>
            T read() {
                T value = *reinterpret_cast<const T*>(data_ + offset_);
                offset_ += sizeof(T);
                return value;
            }

            std::string readString(int32_t size) {
                std::string value(data_ + offset_, size);
                offset_ += size;
                return value;
            }

            void readBytes(int32_t size, char* buffer) {
                memcpy(buffer, data_ + offset_, size);
                offset_ += size;
            }

        private:
            const char* data_;
            int32_t offset_;
        };

        facebook::velox::BufferPtr
        readNulls(int32_t count, ByteStream& stream, facebook::velox::memory::MemoryPool* pool) {
            bool mayHaveNulls = stream.read<bool>();
            if (!mayHaveNulls) {
                return nullptr;
            }

            facebook::velox::BufferPtr nulls = facebook::velox::AlignedBuffer::allocate<bool>(count, pool);

            auto numBytes = facebook::velox::bits::nbytes(count);
            stream.readBytes(numBytes, nulls->asMutable<char>());

            facebook::velox::bits::reverseBits(nulls->asMutable<uint8_t>(), numBytes);
            facebook::velox::bits::negate(nulls->asMutable<char>(), count);
            return nulls;
        }

        template <typename T, typename U>
        facebook::velox::VectorPtr readScalarBlock(
            const facebook::velox::TypePtr& type,
            ByteStream& stream,
            facebook::velox::memory::MemoryPool* pool) {
            auto positionCount = stream.read<int32_t>();

            facebook::velox::BufferPtr nulls = readNulls(positionCount, stream, pool);
            const uint64_t* rawNulls = nulls == nullptr ? nullptr : nulls->as<uint64_t>();

            facebook::velox::BufferPtr buffer =
                facebook::velox::AlignedBuffer::allocate<T>(positionCount, pool);
            auto rawBuffer = buffer->asMutable<T>();
            for (auto i = 0; i < positionCount; i++) {
                if (!rawNulls || !facebook::velox::bits::isBitNull(rawNulls, i)) {
                    rawBuffer[i] = stream.read<T>();
                }
            }

            switch (type->kind()) {
            case facebook::velox::TypeKind::BIGINT:
            case facebook::velox::TypeKind::INTEGER:
            case facebook::velox::TypeKind::SMALLINT:
            case facebook::velox::TypeKind::TINYINT:
            case facebook::velox::TypeKind::DOUBLE:
            case facebook::velox::TypeKind::REAL:
            case facebook::velox::TypeKind::VARCHAR:
            case facebook::velox::TypeKind::DATE:
            case facebook::velox::TypeKind::INTERVAL_DAY_TIME:
                return std::make_shared<facebook::velox::FlatVector<U>>(
                    pool,
                    type,
                    nulls,
                    positionCount,
                    buffer,
                    std::vector<facebook::velox::BufferPtr>{});
            case facebook::velox::TypeKind::TIMESTAMP: {
                facebook::velox::BufferPtr timestamps =
                    facebook::velox::AlignedBuffer::allocate<facebook::velox::Timestamp>(positionCount, pool);
                auto* rawTimestamps = timestamps->asMutable<facebook::velox::Timestamp>();
                for (auto i = 0; i < positionCount; i++) {
                    rawTimestamps[i] = facebook::velox::Timestamp(
                        rawBuffer[i] / 1000, (rawBuffer[i] % 1000) * 1000000);
                }
                return std::make_shared<facebook::velox::FlatVector<facebook::velox::Timestamp>>(
                    pool,
                    type,
                    nulls,
                    positionCount,
                    timestamps,
                    std::vector<facebook::velox::BufferPtr>{});
            }
            case facebook::velox::TypeKind::BOOLEAN: {
                facebook::velox::BufferPtr bits =
                    facebook::velox::AlignedBuffer::allocate<bool>(positionCount, pool);
                auto* rawBits = bits->asMutable<uint64_t>();
                for (auto i = 0; i < positionCount; i++) {
                    facebook::velox::bits::setBit(rawBits, i, rawBuffer[i] != 0);
                }
                return std::make_shared<facebook::velox::FlatVector<bool>>(
                    pool,
                    type,
                    nulls,
                    positionCount,
                    bits,
                    std::vector<facebook::velox::BufferPtr>{});
            }
            default:
                VELOX_FAIL("Unexpected Block type: {}" + type->toString());
            }
        }

        facebook::velox::VectorPtr readVariableWidthBlock(
            const facebook::velox::TypePtr& type,
            ByteStream& stream,
            facebook::velox::memory::MemoryPool* pool) {
            auto positionCount = stream.read<int32_t>();

            facebook::velox::BufferPtr offsets =
                facebook::velox::AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
            auto rawOffsets = offsets->asMutable<int32_t>();
            rawOffsets[0] = 0;
            for (auto i = 0; i < positionCount; i++) {
                rawOffsets[i + 1] = stream.read<int32_t>();
            }

            auto nulls = readNulls(positionCount, stream, pool);

            auto totalSize = stream.read<int32_t>();

            facebook::velox::BufferPtr stringBuffer =
                facebook::velox::AlignedBuffer::allocate<char>(totalSize, pool);
            auto rawString = stringBuffer->asMutable<char>();
            stream.readBytes(totalSize, rawString);

            facebook::velox::BufferPtr buffer =
                facebook::velox::AlignedBuffer::allocate<facebook::velox::StringView>(positionCount, pool);
            auto rawBuffer = buffer->asMutable<facebook::velox::StringView>();
            for (auto i = 0; i < positionCount; i++) {
                auto size = rawOffsets[i + 1] - rawOffsets[i];
                rawBuffer[i] = facebook::velox::StringView(rawString + rawOffsets[i], size);
            }

            return std::make_shared<facebook::velox::FlatVector<facebook::velox::StringView>>(
                pool,
                type,
                nulls,
                positionCount,
                buffer,
                std::vector<facebook::velox::BufferPtr>{stringBuffer});
        }

        template <facebook::velox::TypeKind Kind>
        facebook::velox::VectorPtr readScalarBlock(
            const std::string& encoding,
            const facebook::velox::TypePtr& type,
            ByteStream& stream,
            facebook::velox::memory::MemoryPool* pool) {
            using T = typename facebook::velox::TypeTraits<Kind>::NativeType;

            if (encoding == kLongArray) {
                return readScalarBlock<int64_t, T>(type, stream, pool);
            }

            if (encoding == kIntArray) {
                return readScalarBlock<int32_t, T>(type, stream, pool);
            }

            if (encoding == kShortArray) {
                return readScalarBlock<int16_t, T>(type, stream, pool);
            }

            if (encoding == kByteArray) {
                return readScalarBlock<int8_t, T>(type, stream, pool);
            }

            if (encoding == kVariableWidth) {
                return readVariableWidthBlock(type, stream, pool);
            }

            VELOX_UNREACHABLE();
        }

        facebook::velox::VectorPtr readRleBlock(
            ByteStream& stream,
            facebook::velox::memory::MemoryPool* pool) {
            // read number of rows - must be just one
            auto positionCount = stream.read<int32_t>();

            // skip the encoding of the values
            auto encodingLength = stream.read<int32_t>();
            auto encoding = stream.readString(encodingLength);

            auto innerCount = stream.read<int32_t>();
            VELOX_CHECK_EQ(
                innerCount,
                1,
                "Unexpected RLE block. Expected single inner position. Got {}",
                innerCount);

            auto nulls = readNulls(1, stream, pool);
            if (!nulls || !facebook::velox::bits::isBitNull(nulls->as<uint64_t>(), 0)) {
                throw std::runtime_error("Unexpected RLE block. Expected single null.");
            }

            facebook::velox::TypeKind typeKind;
            if (encoding == kByteArray) {
                typeKind = facebook::velox::TypeKind::UNKNOWN;
            } else if (encoding == kLongArray) {
                typeKind = facebook::velox::TypeKind::BIGINT;
            } else if (encoding == kIntArray) {
                typeKind = facebook::velox::TypeKind::INTEGER;
            } else if (encoding == kShortArray) {
                typeKind = facebook::velox::TypeKind::SMALLINT;
            } else if (encoding == kVariableWidth) {
                typeKind = facebook::velox::TypeKind::VARCHAR;
            } else {
                VELOX_FAIL("Unexpected RLE block encoding: {}", encoding);
            }

            return facebook::velox::BaseVector::createConstant(
                facebook::velox::variant(typeKind), positionCount, pool);
        }

        void unpackTimestampWithTimeZone(
            int64_t packed,
            int64_t& timestamp,
            int16_t& timezone) {
            timestamp = packed >> 12;
            timezone = packed & 0xfff;
        }

        facebook::velox::VectorPtr readBlockInt(
            const facebook::velox::TypePtr& type,
            ByteStream& stream,
            facebook::velox::memory::MemoryPool* pool) {
            // read the encoding
            auto encodingLength = stream.read<int32_t>();
            std::string encoding = stream.readString(encodingLength);

            if (encoding == kArray) {
                auto elements = readBlockInt(type->asArray().elementType(), stream, pool);

                auto positionCount = stream.read<int32_t>();

                facebook::velox::BufferPtr offsets =
                    facebook::velox::AlignedBuffer::allocate<int32_t>(positionCount + 1, pool);
                auto rawOffsets = offsets->asMutable<int32_t>();
                for (auto i = 0; i < positionCount + 1; i++) {
                    rawOffsets[i] = stream.read<int32_t>();
                }

                bool mayHaveNulls = stream.read<bool>();
                VELOX_CHECK(!mayHaveNulls, "Nulls are not supported yet");

                facebook::velox::BufferPtr sizes =
                    facebook::velox::AlignedBuffer::allocate<int32_t>(positionCount, pool);
                auto rawSizes = sizes->asMutable<int32_t>();
                for (auto i = 0; i < positionCount; i++) {
                    rawSizes[i] = rawOffsets[i + 1] - rawOffsets[i];
                }

                return std::make_shared<facebook::velox::ArrayVector>(
                    pool,
                    ARRAY(elements->type()),
                    facebook::velox::BufferPtr(nullptr),
                    positionCount,
                    offsets,
                    sizes,
                    elements);
            }

            if (encoding == kRle) {
                return readRleBlock(stream, pool);
            }

            if (type->kind() == facebook::velox::TypeKind::ROW &&
                isTimestampWithTimeZoneType(type)) {
                auto positionCount = stream.read<int32_t>();

                auto timestamps =
                    facebook::velox::BaseVector::create(facebook::velox::BIGINT(), positionCount, pool);
                auto rawTimestamps =
                    timestamps->asFlatVector<int64_t>()->mutableRawValues();

                auto timezones =
                    facebook::velox::BaseVector::create(facebook::velox::SMALLINT(), positionCount, pool);
                auto rawTimezones = timezones->asFlatVector<int16_t>()->mutableRawValues();

                facebook::velox::BufferPtr nulls = readNulls(positionCount, stream, pool);
                const uint64_t* rawNulls =
                    nulls == nullptr ? nullptr : nulls->as<uint64_t>();

                for (auto i = 0; i < positionCount; i++) {
                    if (!rawNulls || !facebook::velox::bits::isBitNull(rawNulls, i)) {
                        int64_t unpacked = stream.read<int64_t>();
                        unpackTimestampWithTimeZone(
                            unpacked, rawTimestamps[i], rawTimezones[i]);
                    }
                }

                return std::make_shared<facebook::velox::RowVector>(
                    pool,
                    facebook::velox::TIMESTAMP_WITH_TIME_ZONE(),
                    nulls,
                    positionCount,
                    std::vector<facebook::velox::VectorPtr>{timestamps, timezones});
            };

            return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
                readScalarBlock, type->kind(), encoding, type, stream, pool);
        }

    } // namespace

    facebook::velox::VectorPtr readBlock(
        const facebook::velox::TypePtr& type,
        const std::string& base64Encoded,
        facebook::velox::memory::MemoryPool* pool) {
        const std::string data = facebook::velox::encoding::Base64::decode(base64Encoded);

        ByteStream stream(data.data());
        return readBlockInt(type, stream, pool);
    }

} // namespace facebook::trino::protocol

/*
 * Copyright 2026-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <memory>

#include "arrow/array/array_primitive.h"
#include "arrow/ipc/api.h"
#include "paimon/common/data/columnar/columnar_batch_context.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_input_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/disk/file_io_channel.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

class SpillReader : public KeyValueRecordReader {
 public:
    SpillReader(const std::shared_ptr<FileSystem>& fs,
                const std::shared_ptr<arrow::Schema>& key_schema,
                const std::shared_ptr<arrow::Schema>& value_schema,
                const std::shared_ptr<MemoryPool>& pool)
        : fs_(fs),
          key_schema_(key_schema),
          value_schema_(value_schema),
          pool_(pool),
          arrow_pool_(GetArrowPool(pool)),
          metrics_(std::make_shared<MetricsImpl>()) {}

    SpillReader(const SpillReader&) = delete;
    SpillReader& operator=(const SpillReader&) = delete;

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(SpillReader* reader) : reader_(reader) {}

        bool HasNext() const override {
            return cursor_ < reader_->batch_length_;
        }

        Result<KeyValue> Next() override {
            PAIMON_ASSIGN_OR_RAISE(
                const RowKind* row_kind,
                RowKind::FromByteValue(reader_->row_kind_array_->Value(cursor_)));
            int64_t sequence_number = reader_->sequence_number_array_->Value(cursor_);
            auto key = std::make_unique<ColumnarRowRef>(reader_->key_ctx_, cursor_);
            auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, cursor_);
            cursor_++;
            return KeyValue(row_kind, sequence_number, /*level=*/0, std::move(key),
                            std::move(value));
        }

     private:
        int64_t cursor_ = 0;
        SpillReader* reader_ = nullptr;
    };

    Status Open(const std::shared_ptr<FileIOChannel::ID>& channel_id) {
        if (ipc_reader_) {
            return Status::Invalid("SpillReader is already opened");
        }
        auto file_path = channel_id->GetPath();
        PAIMON_ASSIGN_OR_RAISE(auto input_stream, fs_->Open(file_path));
        in_stream_ = std::move(input_stream);
        PAIMON_ASSIGN_OR_RAISE(auto file_status, fs_->GetFileStatus(file_path));
        uint64_t file_len = file_status->GetLen();
        arrow_input_stream_adapter_ =
            std::make_shared<ArrowInputStreamAdapter>(in_stream_, arrow_pool_, file_len);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            ipc_reader_, arrow::ipc::RecordBatchFileReader::Open(arrow_input_stream_adapter_));
        num_record_batches_ = ipc_reader_->num_record_batches();
        current_batch_index_ = 0;
        return Status::OK();
    }

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override {
        Reset();
        if (current_batch_index_ >= num_record_batches_) {
            return std::unique_ptr<KeyValueRecordReader::Iterator>();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::RecordBatch> record_batch,
                                          ipc_reader_->ReadRecordBatch(current_batch_index_));
        current_batch_index_++;

        batch_length_ = record_batch->num_rows();

        auto sequence_field_index =
            record_batch->schema()->GetFieldIndex(SpecialFields::SequenceNumber().Name());
        if (sequence_field_index < 0) {
            return Status::Invalid("cannot find _SEQUENCE_NUMBER column in spill file");
        }
        sequence_number_array_ = std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(
            record_batch->column(sequence_field_index));
        if (!sequence_number_array_) {
            return Status::Invalid("cannot cast _SEQUENCE_NUMBER column to int64 arrow array");
        }

        auto value_kind_index =
            record_batch->schema()->GetFieldIndex(SpecialFields::ValueKind().Name());
        if (value_kind_index < 0) {
            return Status::Invalid("cannot find _VALUE_KIND column in spill file");
        }
        row_kind_array_ = std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(
            record_batch->column(value_kind_index));
        if (!row_kind_array_) {
            return Status::Invalid("cannot cast _VALUE_KIND column to int8 arrow array");
        }

        arrow::ArrayVector key_fields;
        key_fields.reserve(key_schema_->num_fields());
        for (const auto& key_field : key_schema_->fields()) {
            auto col = record_batch->GetColumnByName(key_field->name());
            if (!col) {
                return Status::Invalid("cannot find key field " + key_field->name() +
                                       " in spill file");
            }
            key_fields.emplace_back(col);
        }

        arrow::ArrayVector value_fields;
        value_fields.reserve(value_schema_->num_fields());
        for (const auto& value_field : value_schema_->fields()) {
            auto col = record_batch->GetColumnByName(value_field->name());
            if (!col) {
                return Status::Invalid("cannot find value field " + value_field->name() +
                                       " in spill file");
            }
            value_fields.emplace_back(col);
        }

        key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
        value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);

        return std::make_unique<SpillReader::Iterator>(this);
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return metrics_;
    }

    void Close() override {
        Reset();
        arrow_input_stream_adapter_.reset();
        in_stream_.reset();
        ipc_reader_.reset();
    }

 private:
    void Reset() {
        key_ctx_.reset();
        value_ctx_.reset();
        sequence_number_array_.reset();
        row_kind_array_.reset();
        batch_length_ = 0;
    }

    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<arrow::MemoryPool> arrow_pool_;
    std::shared_ptr<Metrics> metrics_;

    std::shared_ptr<InputStream> in_stream_;
    std::shared_ptr<ArrowInputStreamAdapter> arrow_input_stream_adapter_;
    std::shared_ptr<arrow::ipc::RecordBatchFileReader> ipc_reader_;
    int current_batch_index_ = 0;
    int num_record_batches_ = 0;

    int64_t batch_length_ = 0;
    std::shared_ptr<arrow::NumericArray<arrow::Int64Type>> sequence_number_array_;
    std::shared_ptr<arrow::NumericArray<arrow::Int8Type>> row_kind_array_;
    std::shared_ptr<ColumnarBatchContext> key_ctx_;
    std::shared_ptr<ColumnarBatchContext> value_ctx_;
};

}  // namespace paimon

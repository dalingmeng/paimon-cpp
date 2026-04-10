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

#include "arrow/ipc/api.h"
#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/disk/file_io_channel.h"
#include "paimon/fs/file_system.h"

namespace paimon {

class SpillWriter {
 public:
    SpillWriter(const std::shared_ptr<FileSystem>& fs, const std::shared_ptr<arrow::Schema>& schema,
                const std::shared_ptr<FileIOChannel::Enumerator>& channel_enumerator,
                const std::shared_ptr<SpillChannelManager>& spill_channel_manager,
                const std::string& compression, int32_t compression_level)
        : fs_(fs),
          schema_(schema),
          channel_enumerator_(channel_enumerator),
          spill_channel_manager_(spill_channel_manager),
          compression_(compression),
          compression_level_(compression_level) {}

    SpillWriter(const SpillWriter&) = delete;
    SpillWriter& operator=(const SpillWriter&) = delete;

    Status Open() {
        if (channel_id_) {
            return Status::Invalid("SpillWriter is already opened");
        }
        auto channel_id = channel_enumerator_->Next();
        auto ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto arrow_compression,
                                          arrow::util::Codec::GetCompressionType(compression_));
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            ipc_write_options.codec,
            arrow::util::Codec::Create(arrow_compression, compression_level_));
        PAIMON_ASSIGN_OR_RAISE(out_stream_,
                               fs_->Create(channel_id->GetPath(), /*overwrite=*/false));
        arrow_output_stream_adapter_ = std::make_shared<ArrowOutputStreamAdapter>(out_stream_);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            ipc_writer_,
            arrow::ipc::MakeFileWriter(arrow_output_stream_adapter_, schema_, ipc_write_options));
        channel_id_ = channel_id;
        spill_channel_manager_->AddChannel(channel_id_);
        return Status::OK();
    }

    Status WriteBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(ipc_writer_->WriteRecordBatch(*batch));
        return Status::OK();
    }

    Status Close() {
        if (closed_) {
            return Status::OK();
        }
        PAIMON_RETURN_NOT_OK_FROM_ARROW(ipc_writer_->Close());
        PAIMON_RETURN_NOT_OK(out_stream_->Close());
        closed_ = true;
        return Status::OK();
    }

    Result<int64_t> GetFileSize() const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(int64_t file_size, arrow_output_stream_adapter_->Tell());
        return file_size;
    }

    const std::shared_ptr<FileIOChannel::ID>& GetChannelId() const {
        return channel_id_;
    }

 private:
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<FileIOChannel::Enumerator> channel_enumerator_;
    std::shared_ptr<SpillChannelManager> spill_channel_manager_;
    std::string compression_;
    int32_t compression_level_;
    std::shared_ptr<OutputStream> out_stream_;
    std::shared_ptr<ArrowOutputStreamAdapter> arrow_output_stream_adapter_;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer_;
    std::shared_ptr<FileIOChannel::ID> channel_id_;
    bool closed_ = false;
};

}  // namespace paimon

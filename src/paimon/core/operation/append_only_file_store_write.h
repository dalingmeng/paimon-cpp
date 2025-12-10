/*
 * Copyright 2024-present Alibaba Inc.
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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/abstract_file_store_write.h"
#include "paimon/core/table/bucket_mode.h"
#include "paimon/file_store_write.h"
#include "paimon/logging.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
#include "paimon/type_fwd.h"

struct ArrowSchema;

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class BatchWriter;
class FileStorePathFactory;
class FileStoreScan;
class SnapshotManager;
class ScanFilter;
class MetricsImpl;
class BinaryRow;
class CoreOptions;
class Executor;
class Logger;
class MemoryPool;
class SchemaManager;
class TableSchemaImpl;

class AppendOnlyFileStoreWrite : public AbstractFileStoreWrite {
 public:
    AppendOnlyFileStoreWrite(const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
                             const std::shared_ptr<SnapshotManager>& snapshot_manager,
                             const std::shared_ptr<SchemaManager>& schema_manager,
                             const std::string& commit_user, const std::string& root_path,
                             const std::shared_ptr<TableSchemaImpl>& table_schema,
                             const std::shared_ptr<arrow::Schema>& schema,
                             const std::shared_ptr<arrow::Schema>& write_schema,
                             const std::shared_ptr<arrow::Schema>& partition_schema,
                             const CoreOptions& options, bool ignore_previous_files,
                             bool is_streaming_mode, bool ignore_num_bucket_check,
                             const std::shared_ptr<Executor>& executor,
                             const std::shared_ptr<MemoryPool>& pool);
    ~AppendOnlyFileStoreWrite() override;

 private:
    Result<std::pair<int32_t, std::shared_ptr<BatchWriter>>> CreateWriter(
        const BinaryRow& partition, int32_t bucket, bool ignore_previous_files) override;

    Result<std::unique_ptr<FileStoreScan>> CreateFileStoreScan(
        const std::shared_ptr<ScanFilter>& filter) const override;

 private:
    std::optional<std::vector<std::string>> write_cols_;
    std::unique_ptr<Logger> logger_;
};

}  // namespace paimon

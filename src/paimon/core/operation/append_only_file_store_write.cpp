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

#include "paimon/core/operation/append_only_file_store_write.h"

#include <vector>

#include "paimon/common/data/binary_row.h"
#include "paimon/core/append/append_only_writer.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/manifest/manifest_file.h"
#include "paimon/core/manifest/manifest_list.h"
#include "paimon/core/operation/append_only_file_store_scan.h"
#include "paimon/core/operation/file_store_scan.h"
#include "paimon/core/schema/table_schema_impl.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/logging.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
class DataFilePathFactory;
class Executor;
class MemoryPool;
class SchemaManager;

AppendOnlyFileStoreWrite::AppendOnlyFileStoreWrite(
    const std::shared_ptr<FileStorePathFactory>& file_store_path_factory,
    const std::shared_ptr<SnapshotManager>& snapshot_manager,
    const std::shared_ptr<SchemaManager>& schema_manager, const std::string& commit_user,
    const std::string& root_path, const std::shared_ptr<TableSchemaImpl>& table_schema,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema, const CoreOptions& options,
    bool ignore_previous_files, bool is_streaming_mode, bool ignore_num_bucket_check,
    const std::shared_ptr<Executor>& executor, const std::shared_ptr<MemoryPool>& pool)
    : AbstractFileStoreWrite(file_store_path_factory, snapshot_manager, schema_manager, commit_user,
                             root_path, table_schema, schema, write_schema, partition_schema,
                             options, ignore_previous_files, is_streaming_mode,
                             ignore_num_bucket_check, executor, pool),
      logger_(Logger::GetLogger("AppendOnlyFileStoreWrite")) {
    write_cols_ = write_schema->field_names();
    // optimize write_cols to null in following cases:
    // 1. write_schema contains all columns
    // 2. TODO(xinyu.lxy) write_schema contains all columns and append _ROW_ID & _SEQUENCE_NUMBER
    // cols
    if (schema->Equals(write_schema)) {
        write_cols_ = std::nullopt;
    }
}

AppendOnlyFileStoreWrite::~AppendOnlyFileStoreWrite() = default;

Result<std::unique_ptr<FileStoreScan>> AppendOnlyFileStoreWrite::CreateFileStoreScan(
    const std::shared_ptr<ScanFilter>& scan_filter) const {
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestList> manifest_list,
        ManifestList::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_, pool_));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<ManifestFile> manifest_file,
        ManifestFile::Create(options_.GetFileSystem(), options_.GetManifestFormat(),
                             options_.GetManifestCompression(), file_store_path_factory_,
                             options_.GetManifestTargetFileSize(), pool_, options_,
                             partition_schema_));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStoreScan> scan,
                           AppendOnlyFileStoreScan::Create(
                               snapshot_manager_, schema_manager_, manifest_list, manifest_file,
                               table_schema_, schema_, scan_filter, options_, executor_, pool_));
    return scan;
}

Result<std::pair<int32_t, std::shared_ptr<BatchWriter>>> AppendOnlyFileStoreWrite::CreateWriter(
    const BinaryRow& partition, int32_t bucket, bool ignore_previous_files) {
    PAIMON_LOG_DEBUG(logger_, "Creating append only writer for partition %s, bucket %d",
                     partition.ToString().c_str(), bucket);
    PAIMON_ASSIGN_OR_RAISE(std::optional<Snapshot> latest_snapshot,
                           snapshot_manager_->LatestSnapshot());
    std::vector<std::shared_ptr<DataFileMeta>> restore_files;
    int32_t total_buckets = GetDefaultBucketNum();
    if (!ignore_previous_files && latest_snapshot != std::nullopt) {
        PAIMON_ASSIGN_OR_RAISE(
            total_buckets,
            ScanExistingFileMetas(latest_snapshot.value(), partition, bucket, &restore_files));
    }
    int64_t max_sequence_number = DataFileMeta::GetMaxSequenceNumber(restore_files);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           file_store_path_factory_->CreateDataFilePathFactory(partition, bucket));

    auto writer = std::make_shared<AppendOnlyWriter>(options_, table_schema_->Id(), write_schema_,
                                                     write_cols_, max_sequence_number,
                                                     data_file_path_factory, pool_);
    return std::pair<int32_t, std::shared_ptr<BatchWriter>>(total_buckets, writer);
}

}  // namespace paimon

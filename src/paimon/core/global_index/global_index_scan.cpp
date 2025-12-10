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

#include "paimon/global_index/global_index_scan.h"

#include "paimon/core/core_options.h"
#include "paimon/core/global_index/global_index_scan_impl.h"
#include "paimon/core/schema/schema_manager.h"

namespace paimon {
Result<std::unique_ptr<GlobalIndexScan>> GlobalIndexScan::Create(
    const std::string& root_path, const std::optional<int64_t>& snapshot_id,
    const std::optional<std::vector<std::map<std::string, std::string>>>& partitions,
    const std::map<std::string, std::string>& options,
    const std::shared_ptr<FileSystem>& file_system,
    const std::shared_ptr<MemoryPool>& memory_pool) {
    if (partitions && partitions.value().empty()) {
        return Status::Invalid(
            "invalid input partition, supposed to be null or at least one partition");
    }
    std::shared_ptr<MemoryPool> pool = memory_pool ? memory_pool : GetDefaultPool();
    // load schema
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions tmp_options,
        CoreOptions::FromMap(options, /*fs_scheme_to_identifier_map=*/{}, file_system));
    SchemaManager schema_manager(tmp_options.GetFileSystem(), root_path);
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::shared_ptr<TableSchemaImpl>> latest_table_schema,
                           schema_manager.Latest());
    if (latest_table_schema == std::nullopt) {
        return Status::Invalid("not found latest schema");
    }
    // merge options
    auto final_options = latest_table_schema.value()->Options();
    for (const auto& [key, value] : options) {
        final_options[key] = value;
    }
    PAIMON_ASSIGN_OR_RAISE(
        CoreOptions core_options,
        CoreOptions::FromMap(final_options, /*fs_scheme_to_identifier_map=*/{}, file_system));
    return std::make_unique<GlobalIndexScanImpl>(root_path, latest_table_schema.value(),
                                                 snapshot_id, partitions, core_options, pool);
}

}  // namespace paimon

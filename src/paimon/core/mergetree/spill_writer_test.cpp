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

#include "paimon/core/mergetree/spill_writer.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/core/mergetree/spill_reader.h"
#include "paimon/disk/io_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SpillWriterTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        file_system_ = std::make_shared<LocalFileSystem>();
        test_dir_ = UniqueTestDirectory::Create();

        io_manager_ = IOManager::Create(test_dir_->Str());
        ASSERT_OK_AND_ASSIGN(channel_enumerator_, io_manager_->CreateChannelEnumerator());
        spill_channel_manager_ = std::make_shared<SpillChannelManager>(file_system_);

        // Build write schema: [_SEQUENCE_NUMBER, _VALUE_KIND, key fields..., value fields...]
        value_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                         DataField(1, arrow::field("f1", arrow::int32()))};
        key_fields_ = {DataField(0, arrow::field("f0", arrow::utf8()))};

        std::vector<DataField> write_fields;
        write_fields.push_back(SpecialFields::SequenceNumber());
        write_fields.push_back(SpecialFields::ValueKind());
        write_fields.insert(write_fields.end(), value_fields_.begin(), value_fields_.end());

        write_schema_ = DataField::ConvertDataFieldsToArrowSchema(write_fields);
        write_type_ = DataField::ConvertDataFieldsToArrowStructType(write_fields);

        key_schema_ = DataField::ConvertDataFieldsToArrowSchema(key_fields_);
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(value_fields_);
    }

    std::shared_ptr<arrow::RecordBatch> CreateRecordBatch(const std::string& json_data,
                                                          int64_t num_rows) const {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(write_type_, json_data).ValueOrDie();
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
        return arrow::RecordBatch::Make(write_schema_, num_rows, struct_array->fields());
    }

    std::unique_ptr<SpillWriter> CreateSpillWriter() const {
        return std::make_unique<SpillWriter>(file_system_, write_schema_, channel_enumerator_,
                                             spill_channel_manager_, "zstd",
                                             /*compression_level=*/3);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<FileIOChannel::Enumerator> channel_enumerator_;
    std::shared_ptr<SpillChannelManager> spill_channel_manager_;

    std::vector<DataField> value_fields_;
    std::vector<DataField> key_fields_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<arrow::DataType> write_type_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
};

TEST_F(SpillWriterTest, TestWriteBatch) {
    std::shared_ptr<FileIOChannel::ID> channel_id_1;
    std::shared_ptr<FileIOChannel::ID> channel_id_2;

    // First writer
    {
        auto writer = CreateSpillWriter();
        ASSERT_OK(writer->Open());

        auto batch = CreateRecordBatch(R"([
            [0, 1, "Alice", 10],
            [1, 1, "Bob",   20]
        ])",
                                       2);
        ASSERT_OK(writer->WriteBatch(batch));
        ASSERT_OK_AND_ASSIGN(int64_t file_size, writer->GetFileSize());
        ASSERT_GT(file_size, 0);
        ASSERT_OK(writer->Close());
        channel_id_1 = writer->GetChannelId();
    }
    // Second writer
    {
        auto writer = CreateSpillWriter();
        ASSERT_OK(writer->Open());

        auto batch_a = CreateRecordBatch(R"([
            [2, 1, "Carol", 30],
            [3, 1, "Dave",  40]
        ])",
                                         2);
        auto batch_b = CreateRecordBatch(R"([
            [4, 1, "Eve",   50],
            [5, 1, "Frank", 60],
            [6, 1, "Grace", 70]
        ])",
                                         3);
        ASSERT_OK(writer->WriteBatch(batch_a));
        ASSERT_OK_AND_ASSIGN(int64_t size_before, writer->GetFileSize());
        ASSERT_OK(writer->WriteBatch(batch_b));
        ASSERT_OK_AND_ASSIGN(int64_t size_after, writer->GetFileSize());
        ASSERT_GT(size_after, size_before);
        ASSERT_OK(writer->Close());
        channel_id_2 = writer->GetChannelId();
    }
    // Read back first writer's data
    {
        auto reader =
            std::make_unique<SpillReader>(file_system_, key_schema_, value_schema_, pool_);
        ASSERT_OK(reader->Open(channel_id_1));

        std::vector<std::string_view> expected_keys = {"Alice", "Bob"};
        int total_rows = 0;
        int batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
            if (iter == nullptr) {
                break;
            }
            batch_count++;
            while (iter->HasNext()) {
                ASSERT_OK_AND_ASSIGN(auto kv, iter->Next());
                ASSERT_EQ(kv.key->GetStringView(0), expected_keys[total_rows]);
                total_rows++;
            }
        }
        ASSERT_EQ(batch_count, 1);
        ASSERT_EQ(total_rows, 2);
        reader->Close();
    }
    // Read back second writer's data
    {
        auto reader =
            std::make_unique<SpillReader>(file_system_, key_schema_, value_schema_, pool_);
        ASSERT_OK(reader->Open(channel_id_2));

        std::vector<std::string_view> expected_keys = {"Carol", "Dave", "Eve", "Frank", "Grace"};
        int total_rows = 0;
        int batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
            if (iter == nullptr) {
                break;
            }
            batch_count++;
            while (iter->HasNext()) {
                ASSERT_OK_AND_ASSIGN(auto kv, iter->Next());
                ASSERT_EQ(kv.key->GetStringView(0), expected_keys[total_rows]);
                total_rows++;
            }
        }
        ASSERT_EQ(batch_count, 2);
        ASSERT_EQ(total_rows, 5);
        reader->Close();
    }
}

}  // namespace paimon::test

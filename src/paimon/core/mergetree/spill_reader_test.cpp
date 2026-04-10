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

#include "paimon/core/mergetree/spill_reader.h"

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
#include "paimon/core/mergetree/spill_writer.h"
#include "paimon/disk/io_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SpillReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        file_system_ = std::make_shared<LocalFileSystem>();
        test_dir_ = UniqueTestDirectory::Create();

        io_manager_ = IOManager::Create(test_dir_->Str());
        ASSERT_OK_AND_ASSIGN(channel_enumerator_, io_manager_->CreateChannelEnumerator());
        spill_channel_manager_ = std::make_shared<SpillChannelManager>(file_system_);

        // Schema: [_SEQUENCE_NUMBER(int64), _VALUE_KIND(int8), f0(utf8), f1(int32)]
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

    std::shared_ptr<FileIOChannel::ID> WriteSpillFile(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
        auto writer =
            std::make_unique<SpillWriter>(file_system_, write_schema_, channel_enumerator_,
                                          spill_channel_manager_, "zstd", /*compression_level=*/3);
        EXPECT_OK(writer->Open());
        for (const auto& batch : batches) {
            EXPECT_OK(writer->WriteBatch(batch));
        }
        EXPECT_OK(writer->Close());
        return writer->GetChannelId();
    }

    std::unique_ptr<SpillReader> CreateSpillReader() const {
        return std::make_unique<SpillReader>(file_system_, key_schema_, value_schema_, pool_);
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

TEST_F(SpillReaderTest, TestReadMultipleBatches) {
    auto batch1 = CreateRecordBatch(R"([
        [0, 1, "Alice", 10],
        [1, 1, "Bob",   20]
    ])",
                                    2);
    auto batch2 = CreateRecordBatch(R"([
        [2, 1, "Carol", 30],
        [3, 2, "Dave",  40],
        [4, 3, "Eve",   50]
    ])",
                                    3);

    auto channel_id = WriteSpillFile({batch1, batch2});
    auto reader = CreateSpillReader();
    ASSERT_OK(reader->Open(channel_id));

    std::vector<std::string_view> expected_keys = {"Alice", "Bob", "Carol", "Dave", "Eve"};
    std::vector<int32_t> expected_f1_values = {10, 20, 30, 40, 50};
    std::vector<int64_t> expected_seqs = {0, 1, 2, 3, 4};
    std::vector<int8_t> expected_kinds = {1, 1, 1, 2, 3};

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
            ASSERT_EQ(kv.value->GetStringView(0), expected_keys[total_rows]);
            ASSERT_EQ(kv.value->GetInt(1), expected_f1_values[total_rows]);
            ASSERT_EQ(kv.sequence_number, expected_seqs[total_rows]);
            ASSERT_EQ(kv.value_kind->ToByteValue(), expected_kinds[total_rows]);
            total_rows++;
        }
    }
    ASSERT_EQ(batch_count, 2);
    ASSERT_EQ(total_rows, 5);

    reader->Close();
}

}  // namespace paimon::test

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

#include "paimon/core/mergetree/spill_channel_manager.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "paimon/disk/io_manager.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SpillChannelManagerTest : public ::testing::Test {
 public:
    void SetUp() override {
        file_system_ = std::make_shared<LocalFileSystem>();
        test_dir_ = UniqueTestDirectory::Create();
        io_manager_ = IOManager::Create(test_dir_->Str());
    }

    std::shared_ptr<FileIOChannel::ID> CreateTempFile() {
        auto result = io_manager_->CreateChannel();
        EXPECT_TRUE(result.ok());
        auto channel_id = result.value();
        // Create the actual file on disk
        auto out = file_system_->Create(channel_id->GetPath(), /*overwrite=*/false);
        EXPECT_TRUE(out.ok());
        EXPECT_OK(out.value()->Close());
        return channel_id;
    }

 protected:
    std::shared_ptr<LocalFileSystem> file_system_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::shared_ptr<IOManager> io_manager_;
};

TEST_F(SpillChannelManagerTest, AddAndGetChannels) {
    SpillChannelManager manager(file_system_);

    auto channel1 = CreateTempFile();
    auto channel2 = CreateTempFile();

    manager.AddChannel(channel1);
    manager.AddChannel(channel2);

    const auto& channels = manager.GetChannels();
    ASSERT_EQ(channels.size(), 2);
    ASSERT_GT(channels.count(*channel1), 0);
    ASSERT_GT(channels.count(*channel2), 0);
}

TEST_F(SpillChannelManagerTest, DeleteChannelRemovesFileAndEntry) {
    SpillChannelManager manager(file_system_);

    auto channel = CreateTempFile();
    manager.AddChannel(channel);

    ASSERT_OK_AND_ASSIGN(bool exists_before, file_system_->Exists(channel->GetPath()));
    ASSERT_TRUE(exists_before);

    ASSERT_OK(manager.DeleteChannel(channel));
    ASSERT_EQ(manager.GetChannels().size(), 0);
    ASSERT_OK_AND_ASSIGN(bool exists_after, file_system_->Exists(channel->GetPath()));
    ASSERT_FALSE(exists_after);
}

TEST_F(SpillChannelManagerTest, ResetDeletesAllFiles) {
    SpillChannelManager manager(file_system_);

    auto channel1 = CreateTempFile();
    auto channel2 = CreateTempFile();
    auto channel3 = CreateTempFile();

    manager.AddChannel(channel1);
    manager.AddChannel(channel2);
    manager.AddChannel(channel3);

    ASSERT_OK_AND_ASSIGN(bool e1, file_system_->Exists(channel1->GetPath()));
    ASSERT_OK_AND_ASSIGN(bool e2, file_system_->Exists(channel2->GetPath()));
    ASSERT_OK_AND_ASSIGN(bool e3, file_system_->Exists(channel3->GetPath()));
    ASSERT_TRUE(e1);
    ASSERT_TRUE(e2);
    ASSERT_TRUE(e3);

    ASSERT_OK(manager.Reset());

    ASSERT_OK_AND_ASSIGN(bool a1, file_system_->Exists(channel1->GetPath()));
    ASSERT_OK_AND_ASSIGN(bool a2, file_system_->Exists(channel2->GetPath()));
    ASSERT_OK_AND_ASSIGN(bool a3, file_system_->Exists(channel3->GetPath()));
    ASSERT_FALSE(a1);
    ASSERT_FALSE(a2);
    ASSERT_FALSE(a3);
}

}  // namespace paimon::test

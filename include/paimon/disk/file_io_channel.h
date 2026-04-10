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

#include <cstdint>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <string>

#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/visibility.h"

namespace paimon {

/// A FileIOChannel represents a collection of files that belong logically to the same resource.
/// An example is a collection of files that contain sorted runs of data from the same stream,
/// that will later on be merged together.
class PAIMON_EXPORT FileIOChannel {
 private:
    static constexpr int RANDOM_BYTES_LENGTH = 16;
    static std::string GenerateRandomHexString(std::mt19937& random) {
        std::uniform_int_distribution<int> dist(0, 255);
        std::ostringstream hex_stream;
        hex_stream << std::hex << std::setfill('0');
        for (int i = 0; i < RANDOM_BYTES_LENGTH; ++i) {
            hex_stream << std::setw(2) << dist(random);
        }
        return hex_stream.str();
    }

 public:
    class PAIMON_EXPORT ID {
     public:
        explicit ID(std::string path) : path_(std::move(path)) {}

        ID(const std::string& base_path, std::mt19937& random)
            : path_(base_path + "/" + GenerateRandomHexString(random) + ".channel") {}

        ID(const std::string& base_path, const std::string& prefix, std::mt19937& random)
            : path_(base_path + "/" + prefix + "-" + GenerateRandomHexString(random) + ".channel") {
        }

        const std::string& GetPath() const {
            return path_;
        }

        bool operator==(const ID& other) const {
            return path_ == other.path_;
        }

        bool operator!=(const ID& other) const {
            return !(*this == other);
        }

        struct Hash {
            size_t operator()(const ID& id) const {
                return std::hash<std::string>{}(id.path_);
            }
        };

     private:
        std::string path_;
    };

    class PAIMON_EXPORT Enumerator {
     public:
        Enumerator(std::string base_path, std::mt19937& random)
            : path_(std::move(base_path)), name_prefix_(GenerateRandomHexString(random)) {}
        std::shared_ptr<ID> Next() {
            std::ostringstream filename;
            filename << name_prefix_ << "." << std::setfill('0') << std::setw(6)
                     << (local_counter_++) << ".channel";

            std::string full_path = path_ + "/" + filename.str();
            return std::make_shared<ID>(std::move(full_path));
        }

     private:
        std::string path_;
        std::string name_prefix_;
        uint64_t local_counter_{0};
    };
};

}  // namespace paimon

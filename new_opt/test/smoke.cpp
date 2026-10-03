// Copyright 2026 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>

#include "link.h"
#include "scheduler.h"

int main() {
    opt::SLink link;
    const bool link_is_empty = link._next == nullptr && link._prev == nullptr;
    const auto runnable = opt::Scheduler::JobResultP::JobResult_Runnable;
    const bool enum_is_stable = static_cast<std::uint32_t>(runnable) == 0;
    return link_is_empty && enum_is_stable ? 0 : 1;
}

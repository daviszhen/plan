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

#include <cstdlib>
#include <iostream>

#include "job.h"
#include "scheduler.h"
#include "engine.h"

namespace {

void Check(bool condition, const char *message) {
  if (!condition) {
    std::cerr << "check failed: " << message << '\n';
    std::exit(EXIT_FAILURE);
  }
}

struct Fixture {
  Fixture() : context(), engine() {
    context.Init(&factory, &scheduler, &engine);
  }

  opt::JobFactory factory{32};
  opt::Scheduler scheduler;
  opt::SchedulerCtx context;
  opt::Engine engine;
};

void TestContextAccessors() {
  Fixture fixture;
  Check(fixture.context.Pjf() == &fixture.factory, "factory accessor");
  Check(fixture.context.Psched() == &fixture.scheduler,
        "scheduler accessor");
  Check(fixture.context.Peng() == &fixture.engine, "engine accessor");
}

void TestSpawnedJobsResumeTheirParent() {
  Fixture fixture;
  opt::JobTest::ResetCnt();

  opt::Job *job = fixture.factory.PjCreate(opt::Job::EjtTest);
  auto *root = opt::JobTest::PjConvert(job);
  root->Init(opt::JobTest::EttSpawn,
             /*rounds=*/1,
             /*fanout=*/3,
             /*iters=*/0,
             /*queue=*/nullptr);
  fixture.scheduler.Add(job, nullptr);

  Check(opt::Scheduler::Run(&fixture.context) == nullptr,
        "scheduler run returns null");
  // One root execution creates three children.  Once all children complete,
  // the root is resumed and executes once more to finish.
  Check(opt::JobTest::UlpCnt() == 5,
        "spawned children and resumed parent execute exactly once");
}

void TestQueuedJobsCompleteAndResumeParent() {
  Fixture fixture;
  opt::JobTest::ResetCnt();
  opt::JobQueue queue;

  opt::Job *job = fixture.factory.PjCreate(opt::Job::EjtTest);
  auto *root = opt::JobTest::PjConvert(job);
  root->Init(opt::JobTest::EttStartQueue,
             /*rounds=*/1,
             /*fanout=*/4,
             /*iters=*/0,
             &queue);
  fixture.scheduler.Add(job, nullptr);

  Check(opt::Scheduler::Run(&fixture.context) == nullptr,
        "queued scheduler run returns null");
  // The root first starts the queue and is suspended until all queue jobs
  // finish.  It is then resumed once and completes on the second call.
  Check(opt::JobTest::UlpCnt() == 2,
        "queued root is resumed after its children complete");
}

} // namespace

int main() {
  TestContextAccessors();
  TestSpawnedJobsResumeTheirParent();
  TestQueuedJobsCompleteAndResumeParent();
  return EXIT_SUCCESS;
}

// Copyright (C) 2025 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull;

import com.google.gerrit.acceptance.SkipProjectClone;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.UseLocalDisk;
import java.util.List;

@SkipProjectClone
@UseLocalDisk
@TestPlugin(
    name = "pull-replication",
    sysModule = "com.gerritforge.gerrit.plugins.replication.pull.TestPullReplicationModule",
    httpModule = "com.gerritforge.gerrit.plugins.replication.pull.api.HttpModule")
public class AsyncPullReplicationBatchRefUpdatedIT extends PullReplicationITBase {
  private static final int ASYNC_REPLICATION_DELAY_SEC = 2;

  @Override
  protected List<String> syncRefs() {
    return List.of("NONE");
  }

  @Override
  protected int replicationDelaySec() {
    return ASYNC_REPLICATION_DELAY_SEC;
  }

  @Override
  protected boolean useBatchRefUpdateEvent() {
    return true;
  }
}

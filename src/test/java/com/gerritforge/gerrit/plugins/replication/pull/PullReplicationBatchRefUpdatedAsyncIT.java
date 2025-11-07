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
import com.google.gerrit.server.config.SitePaths;
import com.google.inject.Inject;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;

@SkipProjectClone
@UseLocalDisk
@TestPlugin(
    name = "pull-replication",
    sysModule =
        "com.gerritforge.gerrit.plugins.replication.pull.PullReplicationITAbstract$PullReplicationTestModule",
    httpModule = "com.gerritforge.gerrit.plugins.replication.pull.api.HttpModule")
public class PullReplicationBatchRefUpdatedAsyncIT extends PullReplicationITAbstract {
  @Inject private SitePaths sitePaths;

  @Override
  protected boolean useBatchRefUpdateEvent() {
    return true;
  }

  @Override
  public void setUpTestPlugin() throws Exception {
    FileBasedConfig config =
        new FileBasedConfig(sitePaths.etc_dir.resolve("replication.config").toFile(), FS.DETECTED);
    config.setString("replication", null, "syncRefs", "^$");
    config.save();

    super.setUpTestPlugin(true);
  }
}

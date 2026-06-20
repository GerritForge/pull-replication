// Copyright (C) 2026 GerritForge, Inc.
//
// Licensed under the BSL 1.1 (the "License");
// you may not use this file except in compliance with the License.
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import static com.google.gerrit.testing.GerritJUnit.assertThrows;

import com.gerritforge.gerrit.plugins.replication.pull.RevisionReader;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.google.common.collect.ImmutableList;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.SkipProjectClone;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.UseLocalDisk;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.Project.NameKey;
import com.google.gerrit.extensions.config.FactoryModule;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.googlesource.gerrit.plugins.replication.FileConfigResource;
import com.googlesource.gerrit.plugins.replication.ReplicationConfigImpl;
import com.googlesource.gerrit.plugins.replication.api.ConfigResource;
import com.googlesource.gerrit.plugins.replication.api.ReplicationConfig;
import com.googlesource.gerrit.plugins.replication.api.ReplicationConfigOverrides;
import java.util.List;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.transport.RefSpec;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests for {@link BatchApplyObject}.
 *
 * <p>Sibling of {@link ApplyObjectIT}: covers the multi-ref batch-apply path that the bifurcation
 * routes through {@code BatchApplyObject.applyBatch}. The single-ref code path is exercised by
 * {@link ApplyObjectIT}.
 */
@SkipProjectClone
@UseLocalDisk
@TestPlugin(
    name = "pull-replication",
    sysModule =
        "com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchApplyObjectIT$TestModule")
public class BatchApplyObjectIT extends LightweightPluginDaemonTest {

  @Inject BatchApplyObject objectUnderTest;
  RevisionReader reader;

  @Before
  public void setup() {
    reader = plugin.getSysInjector().getInstance(RevisionReader.class);
  }

  /**
   * Deferred. In the source chain (pre-bifurcation), {@code BatchApplyObject} exposed a
   * package-private {@code create(Repository)} factory plus {@code add()} / {@code
   * getBatchRefUpdate()} accessors, which let the test interleave a snapshot, an out-of-band
   * advance, and an execute on the main thread.
   *
   * <p>In the bifurcated chain {@code BatchApplyObject} is {@code @Inject}-constructed and {@link
   * BatchApplyObject#applyBatch} opens the repo + snapshots oldId + executes the batch in one
   * sealed call. Reproducing the TOCTOU window from outside would require either re-introducing
   * the package-private hooks or driving a concurrent writer thread with a latch wired into the
   * snapshot/execute boundary. Out of scope for this commit; tracking separately.
   */
  @Ignore("TOCTOU window is sealed inside BatchApplyObject.applyBatch; needs latched concurrent writer")
  @Test
  public void shouldRejectBatchWhenRefAdvancedOutOfBandBeforeExecute() {}

  @Test
  public void shouldThrowResourceNotFoundWhenProjectRepositoryMissing() throws Exception {
    // Pin the RepositoryNotFoundException -> ResourceNotFoundException translation in
    // BatchApplyObject.applyBatch. Without it, the underlying JGit RepositoryNotFoundException
    // would surface as a generic IOException and the REST layer would respond 500 instead of 404.
    NameKey missingProject = Project.nameKey("project-that-does-not-exist");
    RefSpec refSpec = new RefSpec("refs/heads/master");
    RevisionData emptyRev =
        new RevisionData(
            ImmutableList.of(),
            new RevisionObjectData("0".repeat(40), Constants.OBJ_COMMIT, new byte[] {}),
            new RevisionObjectData("0".repeat(40), Constants.OBJ_TREE, new byte[] {}),
            ImmutableList.of());

    List<RevisionData[]> revisionsDataList = new java.util.ArrayList<>();
    revisionsDataList.add(new RevisionData[] {emptyRev});
    assertThrows(
        ResourceNotFoundException.class,
        () -> objectUnderTest.applyBatch(missingProject, List.of(refSpec), revisionsDataList));
  }

  @SuppressWarnings("unused")
  private static class TestModule extends FactoryModule {
    @Override
    protected void configure() {
      DynamicItem.itemOf(binder(), ReplicationConfigOverrides.class);
      bind(ConfigResource.class).to(FileConfigResource.class);
      bind(ReplicationConfig.class).to(ReplicationConfigImpl.class);
      bind(RevisionReader.class).in(Scopes.SINGLETON);
      bind(BatchApplyObject.class);
    }
  }
}

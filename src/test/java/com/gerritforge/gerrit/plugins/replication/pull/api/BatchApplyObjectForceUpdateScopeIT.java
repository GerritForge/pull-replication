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

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.google.common.truth.Truth.assertThat;
import static com.google.gerrit.testing.GerritJUnit.assertThrows;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.google.common.collect.ImmutableList;
import com.google.gerrit.acceptance.PushOneCommit;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.RefNames;
import com.google.gerrit.extensions.restapi.ResourceConflictException;
import com.google.gerrit.extensions.restapi.Url;
import com.google.gerrit.server.IdentifiedUser;
import com.google.gerrit.server.project.ProjectResource;
import com.google.gerrit.server.project.ProjectState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectInserter;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.junit.Test;

/**
 * Discriminating regression test for the scope of the "force update" flag on a multi-ref batch.
 *
 * <p>{@code BatchApplyObject#add} currently flips {@code bru.setAllowNonFastForwards(true)} on the
 * whole {@link org.eclipse.jgit.lib.BatchRefUpdate} whenever any input in the batch is a
 * non-commit (blob) ref. That per-batch scope leaks into every other ref's command, so a non-FF
 * update on a commit ref that shares a batch with a blob ref silently lands.
 *
 * <p>This test ships a batch with two refs:
 *
 * <ol>
 *   <li>{@code refs/heads/master} ← rewound to an earlier OID (non-FF on a commit ref)
 *   <li>{@code refs/test/blob} ← updated to a new blob (non-FF on a blob ref, which is the input
 *       that triggers {@code setAllowNonFastForwards(true)})
 * </ol>
 *
 * <p>Expected behavior under a correct implementation -- force-update flag is per-{@link
 * org.eclipse.jgit.transport.ReceiveCommand} (set via {@code cmd.setForceUpdate(true)} only on the
 * blob command):
 *
 * <ul>
 *   <li>Master command: {@code REJECTED_NONFASTFORWARD}.
 *   <li>Blob command: {@code REJECTED_OTHER_REASON} (transaction aborted).
 *   <li>BatchRefUpdateException → UnprocessableEntityException at the REST layer.
 *   <li>Neither ref moves.
 * </ul>
 *
 * <p>Current (buggy) behavior -- per-batch {@code setAllowNonFastForwards(true)}:
 *
 * <ul>
 *   <li>Master command silently allowed; master rewinds.
 *   <li>Blob command silently allowed; blob updates.
 *   <li>No exception; batch returns success.
 * </ul>
 *
 * <p>The test asserts the correct behavior, so it fails on the current chain and passes after the
 * per-command {@code setForceUpdate} fix in {@code BatchApplyObject}.
 */
public class BatchApplyObjectForceUpdateScopeIT extends ActionITBase {

  private static final String MASTER_REF = RefNames.REFS_HEADS + "master";
  private static final String BLOB_REF = "refs/test/blob";
  private static final long EVENT_TS = 1L;

  @Override
  protected String getURLWithAuthenticationPrefix(String projectName) {
    return String.format(
        "%s/a/projects/%s/pull-replication~batch-apply-object",
        adminRestSession.url(), Url.encode(projectName));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void blobInBatchMustNotEnableForceForCommitRefs() throws Exception {
    testRepo = cloneProject(project);

    // Snapshot initial master OID and advance master so the rewind ships will be non-FF.
    ObjectId initialMasterOid = readRefOid(project, MASTER_REF);
    PushOneCommit advance =
        pushFactory.create(admin.newIdent(), testRepo, "advance master", "f.txt", "x");
    advance.to(MASTER_REF).assertOkStatus();
    ObjectId advancedMasterOid = readRefOid(project, MASTER_REF);
    assertThat(advancedMasterOid).isNotEqualTo(initialMasterOid);

    // Seed a blob ref so the batch's update is blob-to-blob (non-FF for blobs).
    ObjectId initialBlobOid = createBlobRef(project, BLOB_REF, "initial");

    // Batch:
    //  master ← rewind to initial OID (non-FF; should be rejected on a correct impl)
    //  blob ref ← new blob content (non-FF blob; triggers setAllowNonFastForwards today)
    RevisionData rewindRev = readRevAt(project, MASTER_REF, initialMasterOid);
    RevisionData newBlobRev = blobOnlyRevisionData("new content");

    List<RevisionInput> inputs =
        List.of(
            new RevisionInput(TEST_REPLICATION_REMOTE, MASTER_REF, EVENT_TS, rewindRev),
            new RevisionInput(TEST_REPLICATION_REMOTE, BLOB_REF, EVENT_TS, newBlobRev));

    BatchApplyObjectAction action =
        plugin.getSysInjector().getInstance(BatchApplyObjectAction.class);
    ProjectResource resource = projectResource(project);

    // Discriminator: BatchApplyObject's pre-flight FF check on commit refs throws
    // NonFastForwardException for the master rewind before bru.execute() runs, which
    // BatchApplyObjectAction translates to ResourceConflictException (HTTP 409). Without
    // the pre-flight check the per-batch setAllowNonFastForwards(true) flipped by the
    // blob input would silently permit the rewind and no exception would be thrown.
    assertThrows(ResourceConflictException.class, () -> action.apply(resource, inputs));

    // Neither ref moved.
    assertThat(readRefOid(project, MASTER_REF)).isEqualTo(advancedMasterOid);
    assertThat(readRefOid(project, BLOB_REF)).isEqualTo(initialBlobOid);
  }

  private ProjectResource projectResource(Project.NameKey name) {
    ProjectState state = projectCache.get(name).orElseThrow();
    IdentifiedUser user = identifiedUserFactory.create(admin.id());
    return new ProjectResource(state, user);
  }

  private RevisionData readRevAt(Project.NameKey p, String ref, ObjectId oid) throws Exception {
    Optional<RevisionData> read;
    try (Repository repo = repoManager.openRepository(p)) {
      read = revisionReader.read(p, oid, ref, 0);
    }
    return read.orElseThrow();
  }

  private ObjectId readRefOid(Project.NameKey p, String ref) throws Exception {
    try (Repository repo = repoManager.openRepository(p)) {
      Ref r = repo.exactRef(ref);
      return r == null ? null : r.getObjectId();
    }
  }

  private ObjectId createBlobRef(Project.NameKey p, String refName, String content)
      throws Exception {
    try (Repository repo = repoManager.openRepository(p)) {
      ObjectId blobOid;
      try (ObjectInserter ins = repo.newObjectInserter()) {
        blobOid = ins.insert(Constants.OBJ_BLOB, content.getBytes(StandardCharsets.UTF_8));
        ins.flush();
      }
      RefUpdate ru = repo.updateRef(refName);
      ru.setNewObjectId(blobOid);
      ru.setExpectedOldObjectId(ObjectId.zeroId());
      RefUpdate.Result result = ru.update();
      assertThat(result).isAnyOf(RefUpdate.Result.NEW, RefUpdate.Result.FORCED);
      return blobOid;
    }
  }

  private RevisionObjectData blobOnly(String content) {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    // sha1 is informational; ApplyObject computes the real OID via ObjectInserter.
    return new RevisionObjectData("0".repeat(40), Constants.OBJ_BLOB, bytes);
  }

  private RevisionData blobOnlyRevisionData(String content) {
    return new RevisionData(ImmutableList.of(), null, null, ImmutableList.of(blobOnly(content)));
  }
}

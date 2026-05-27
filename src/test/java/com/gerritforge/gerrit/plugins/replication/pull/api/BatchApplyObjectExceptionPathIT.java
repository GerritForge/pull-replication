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
import com.google.gerrit.acceptance.PushOneCommit;
import com.google.gerrit.acceptance.config.GerritConfig;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.RefNames;
import com.google.gerrit.extensions.restapi.ResourceConflictException;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.Url;
import com.google.gerrit.server.IdentifiedUser;
import com.google.gerrit.server.project.ProjectResource;
import com.google.gerrit.server.project.ProjectState;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.junit.Test;

/**
 * Pins atomicity and error-type fidelity for a non-fast-forward update on a commit ref in a
 * multi-ref batch.
 *
 * <p>Setup: advance master via a direct push, then ship the initial master OID for masterRef in
 * the batch (a rewind = non-FF), plus a fresh branch CREATE for a second ref.
 *
 * <p>Two contracts pinned:
 *
 * <ol>
 *   <li><b>Atomicity</b>: master must stay at the advanced OID; the secondary branch must not be
 *       created. Holds via the pre-flight FF check in {@code BatchApplyObject.add} and JGit's
 *       default {@code atomic=true} on {@code RefDirectory} / reftable.
 *   <li><b>Error-type fidelity</b>: non-FF rejection surfaces as {@link ResourceConflictException}
 *       (HTTP 409) via the {@code NonFastForwardException -> catch} arm in {@code
 *       BatchApplyObjectAction}, <b>not</b> as a generic {@link RestApiException} from {@code
 *       RestApiException.wrap(IOException)}.
 * </ol>
 *
 * <p>Historical context (kept as commit-history detail; the test now exercises the pre-flight
 * path, not the execute-time path):
 *
 * <ul>
 *   <li>Originally this test was the discriminator for the {@code executeChecked} vs {@code
 *       execute(rw, monitor)} fix in {@code ApplyObject.applyBatch}. With {@code executeChecked}
 *       in place, a mid-execute {@code REJECTED_NONFASTFORWARD} produced {@code
 *       GitUpdateFailureException} (an {@code IOException}) which bypassed the chain's per-ref
 *       result mapping and surfaced as generic {@link RestApiException}.
 *   <li>The {@code executeChecked -> execute} fix landed, then the {@code BatchApplyObject}
 *       pre-flight FF check landed. The pre-flight check now intercepts non-FF rejections on
 *       commit refs before {@code bru.execute()} is called, throwing {@code
 *       NonFastForwardException} that maps to {@link ResourceConflictException}.
 *   <li>An equivalent test that exercises the {@code execute(rw, monitor)} failure path now
 *       requires triggering {@code LOCK_FAILURE} via concurrent writers -- that test is deferred
 *       (P1.3 in the working plan).
 * </ul>
 */
public class BatchApplyObjectExceptionPathIT extends ActionITBase {

  private static final String MASTER_REF = RefNames.REFS_HEADS + "master";
  private static final String NEW_BRANCH_REF = RefNames.REFS_HEADS + "test-branch";
  private static final long EVENT_TS = 1L;

  @Override
  protected String getURLWithAuthenticationPrefix(String projectName) {
    return String.format(
        "%s/a/projects/%s/pull-replication~batch-apply-object",
        adminRestSession.url(), Url.encode(projectName));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void batchMustAbortAtomicallyOnNonFastForwardCommitRef() throws Exception {
    testRepo = cloneProject(project);

    // Snapshot master's initial OID.
    ObjectId initialMasterOid = readRefOid(project, MASTER_REF);
    assertThat(initialMasterOid).isNotNull();

    // Advance master via a direct push (admin can push direct on the test project).
    PushOneCommit advance =
        pushFactory.create(admin.newIdent(), testRepo, "advance master", "f.txt", "content");
    PushOneCommit.Result advanceResult = advance.to(MASTER_REF);
    advanceResult.assertOkStatus();

    ObjectId advancedMasterOid = readRefOid(project, MASTER_REF);
    assertThat(advancedMasterOid).isNotEqualTo(initialMasterOid);

    // Read RevisionData at the *old* OID. Shipping this for masterRef would rewind master
    // (advancedMasterOid -> initialMasterOid), which is non-FF and rejected at execute-time
    // because no per-ReceiveCommand setForceUpdate is set.
    RevisionData rewindToInitial = readRevAt(project, MASTER_REF, initialMasterOid);

    // Read RevisionData at the *new* OID. Shipping this for a fresh branch would CREATE the
    // branch successfully on its own.
    RevisionData advancedRev = readRevAt(project, MASTER_REF, advancedMasterOid);

    List<RevisionInput> inputs =
        List.of(
            new RevisionInput(TEST_REPLICATION_REMOTE, MASTER_REF, EVENT_TS, rewindToInitial),
            new RevisionInput(TEST_REPLICATION_REMOTE, NEW_BRANCH_REF, EVENT_TS, advancedRev));

    BatchApplyObjectAction action =
        plugin.getSysInjector().getInstance(BatchApplyObjectAction.class);
    ProjectResource resource = projectResource(project);

    // ===== Contract 2: error-type fidelity. =====
    // BatchApplyObject's pre-flight FF check throws NonFastForwardException for the
    // master rewind before bru.execute() is called. BatchApplyObjectAction maps that
    // to ResourceConflictException (HTTP 409). Without the pre-flight check, a
    // mid-execute rejection would surface either as UnprocessableEntityException
    // (the executeChecked -> execute fix) or as plain RestApiException.wrap(IOException)
    // (pre-fix). This test pins the current contract: 409 via pre-flight rejection.
    assertThrows(ResourceConflictException.class, () -> action.apply(resource, inputs));

    // ===== Contract 1: atomicity. =====
    // Holds regardless of which exception path was taken, because JGit's BatchRefUpdate is
    // atomic-by-default on RefDirectory/reftable. Pin it explicitly so a future regression in
    // atomicity (e.g. someone removes setAtomic) is caught even on the buggy exception path.
    assertThat(readRefOid(project, MASTER_REF)).isEqualTo(advancedMasterOid);
    assertThat(readRef(project, NEW_BRANCH_REF)).isNull();
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
    Ref r = readRef(p, ref);
    return r == null ? null : r.getObjectId();
  }

  private Ref readRef(Project.NameKey p, String ref) throws Exception {
    try (Repository repo = repoManager.openRepository(p)) {
      return repo.exactRef(ref);
    }
  }
}

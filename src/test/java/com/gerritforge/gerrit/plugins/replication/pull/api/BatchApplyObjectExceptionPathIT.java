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
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
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
 * Discriminating regression test for the {@code executeChecked} vs {@code execute(rw, monitor)}
 * question in {@code ApplyObject.applyBatch}.
 *
 * <p>Triggers a <b>mid-execute</b> failure (REJECTED_NONFASTFORWARD via a rewind), as opposed to
 * the pre-flight parent-check failure exercised by {@link BatchApplyObjectAtomicityIT}.
 *
 * <p>Two contracts checked:
 *
 * <ol>
 *   <li><b>Atomicity</b>: when one ref's command is rejected by {@code BatchRefUpdate.execute()},
 *       the other ref in the batch must not have landed. This is the JGit-level invariant
 *       (atomic-by-default on {@code RefDirectory} / reftable) and should hold regardless of how
 *       the rejection is propagated above.
 *   <li><b>Error-type fidelity</b>: a rejected ref-update at execute-time must surface as a {@link
 *       UnprocessableEntityException} (the chain's {@code BatchRefUpdateException} -> 422 path),
 *       <b>not</b> as a generic {@link RestApiException} from {@code
 *       RestApiException.wrap(IOException)}. The latter is the symptom of {@code
 *       RefUpdateUtil.executeChecked} throwing {@code GitUpdateFailureException} (which extends
 *       {@code IOException}) and bypassing the chain's per-ref result mapping.
 * </ol>
 *
 * <p>Expected results vs implementation:
 *
 * <ul>
 *   <li>On the chain at HEAD (uses {@code RefUpdateUtil.executeChecked} in {@code
 *       ApplyObject.applyBatch}): atomicity holds, but the exception is a base {@code
 *       RestApiException} - this test <b>fails</b> on the {@code assertThrows
 *       (UnprocessableEntityException.class, ...)} assertion. That failure is the proof that the
 *       {@code BatchRefUpdateException} machinery introduced in chain commits 2-3 is unreachable.
 *   <li>After replacing {@code executeChecked(bru, git)} with {@code bru.execute(rw,
 *       NullProgressMonitor.INSTANCE)}: this test <b>passes</b> end-to-end.
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
  public void batchMustAbortAtomicallyOnMidExecuteRejection() throws Exception {
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
    // The chain's BatchApplyObjectAction catch sites translate:
    //   BatchRefUpdateException     -> UnprocessableEntityException (422)
    //   IOException                 -> RestApiException.wrap(...)   (base type)
    // With executeChecked in place, REJECTED_NONFASTFORWARD becomes GitUpdateFailureException
    // (IOException), so we land in the wrap path - this assertion fails.
    // With bru.execute(rw, monitor), the BatchRefUpdateException path fires - this passes.
    assertThrows(UnprocessableEntityException.class, () -> action.apply(resource, inputs));

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

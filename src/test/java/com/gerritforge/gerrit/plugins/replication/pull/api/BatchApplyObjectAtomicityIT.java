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
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.Url;
import com.google.gerrit.server.IdentifiedUser;
import com.google.gerrit.server.project.ProjectResource;
import com.google.gerrit.server.project.ProjectState;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.junit.Test;

/**
 * Demonstrates the partial-apply / atomicity contract of {@code batch-apply-object}.
 *
 * <p>Posts a 3-ref batch where the last input is a meta commit referencing a parent SHA that does
 * not exist in the local object database. The first two inputs (master, patch-set) would otherwise
 * succeed on their own.
 *
 * <p>Contract under test: when any ref in the batch fails, <b>no</b> ref in the batch may have
 * advanced. Concretely, {@code refs/heads/master} must still point at its pre-call OID.
 *
 * <p>On the per-ref-loop version of {@code BatchApplyObjectAction} (pre-refactor master), this test
 * fails -- master advances before the meta input throws. On the {@code BatchRefUpdate}-backed
 * version, the meta parent check fires before {@code execute()} is invoked, so the batch is
 * abandoned and master stays.
 *
 * <p>The action is invoked programmatically (no HTTP / JSON) to keep the harness minimal -- see
 * {@link ApplyObjectActionIT} for the HTTP-shaped equivalent.
 */
public class BatchApplyObjectAtomicityIT extends ActionITBase {

  private static final String FAKE_PARENT_SHA = "dead0000dead0000dead0000dead0000dead0000";
  private static final String MASTER_REF = RefNames.REFS_HEADS + "master";
  private static final long EVENT_TS = 1L;

  @Override
  protected String getURLWithAuthenticationPrefix(String projectName) {
    // Not used by this IT -- the action is invoked programmatically. Returning a
    // batch-apply-object URL keeps the abstract method satisfied and matches the
    // convention of the sibling ApplyObjectActionIT.
    return String.format(
        "%s/a/projects/%s/pull-replication~batch-apply-object",
        adminRestSession.url(), Url.encode(projectName));
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void batchMustNotPartiallyApplyWhenOneRefFails() throws Exception {
    testRepo = cloneProject(project);
    PushOneCommit.Result push = createChange();

    String patchSetRef = RefNames.patchSetRef(push.getPatchSetId());
    String metaRef = RefNames.changeMetaRef(push.getChange().getId());

    // Read the valid RevisionData for the patch-set commit. We ship it twice in
    // the batch: once as the master input (so master would advance from its
    // initial OID to the patch-set commit, if the batch ran ref-by-ref) and once
    // as the patch-set input itself (a no-op).
    RevisionData validRev = readRev(project, patchSetRef);

    ObjectId masterBefore = readRefOid(project, MASTER_REF);
    assertThat(masterBefore).isNotNull();
    assertThat(masterBefore.name()).isNotEqualTo(validRev.getCommitObject().getSha1());

    RevisionData brokenMeta = brokenMetaWith(validRev.getTreeObject().getSha1(), FAKE_PARENT_SHA);

    List<RevisionInput> inputs =
        List.of(
            new RevisionInput(TEST_REPLICATION_REMOTE, MASTER_REF, EVENT_TS, validRev),
            new RevisionInput(TEST_REPLICATION_REMOTE, patchSetRef, EVENT_TS, validRev),
            new RevisionInput(TEST_REPLICATION_REMOTE, metaRef, EVENT_TS, brokenMeta));

    BatchApplyObjectAction action =
        plugin.getSysInjector().getInstance(BatchApplyObjectAction.class);
    ProjectResource resource = projectResource(project);

    assertThrows(RestApiException.class, () -> action.apply(resource, inputs));

    // The contract: master must not have advanced. Fails on the per-ref-loop
    // version of BatchApplyObjectAction (master moves to validRev's commit before
    // the meta input throws); passes once all refs are queued into one
    // BatchRefUpdate and the meta parent check aborts before execute().
    ObjectId masterAfter = readRefOid(project, MASTER_REF);
    assertThat(masterAfter).isEqualTo(masterBefore);
  }

  @Test
  @GerritConfig(name = "gerrit.instanceId", value = "testInstanceId")
  public void batchMustNotPartiallyApplyWhenNonMetaCommitHasMissingParent() throws Exception {
    // Same contract as batchMustNotPartiallyApplyWhenOneRefFails, but the
    // broken ref is a patch-set ref (refs/changes/.../1), not /meta. Pins that
    // BatchApplyObject's parent-existence check fires for any commit ref --
    // not just /meta -- and aborts the whole batch atomically.
    testRepo = cloneProject(project);
    PushOneCommit.Result push = createChange();

    String patchSetRef = RefNames.patchSetRef(push.getPatchSetId());
    String metaRef = RefNames.changeMetaRef(push.getChange().getId());

    RevisionData validRev = readRev(project, patchSetRef);

    ObjectId masterBefore = readRefOid(project, MASTER_REF);
    assertThat(masterBefore).isNotNull();

    // Patch-set commit body with a parent SHA absent from the local ODB.
    // ChangeMetaCommitValidator only fires on /meta refs, so for a patch-set
    // ref the parent-existence check is the only pre-flight gate -- exactly
    // the property we want to pin.
    RevisionData brokenPatchSet =
        brokenMetaWith(validRev.getTreeObject().getSha1(), FAKE_PARENT_SHA);

    List<RevisionInput> inputs =
        List.of(
            new RevisionInput(TEST_REPLICATION_REMOTE, MASTER_REF, EVENT_TS, validRev),
            new RevisionInput(TEST_REPLICATION_REMOTE, patchSetRef, EVENT_TS, brokenPatchSet),
            new RevisionInput(TEST_REPLICATION_REMOTE, metaRef, EVENT_TS, validRev));

    BatchApplyObjectAction action =
        plugin.getSysInjector().getInstance(BatchApplyObjectAction.class);
    ProjectResource resource = projectResource(project);

    assertThrows(RestApiException.class, () -> action.apply(resource, inputs));

    ObjectId masterAfter = readRefOid(project, MASTER_REF);
    assertThat(masterAfter).isEqualTo(masterBefore);
  }

  private ProjectResource projectResource(Project.NameKey name) {
    ProjectState state = projectCache.get(name).orElseThrow();
    IdentifiedUser user = identifiedUserFactory.create(admin.id());
    return new ProjectResource(state, user);
  }

  private RevisionData readRev(Project.NameKey p, String ref) throws Exception {
    Optional<RevisionData> read;
    try (Repository repo = repoManager.openRepository(p)) {
      ObjectId oid = repo.exactRef(ref).getObjectId();
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

  private static RevisionData brokenMetaWith(String treeSha, String fakeParentSha) {
    byte[] commitBody =
        ("tree "
                + treeSha
                + "\n"
                + "parent "
                + fakeParentSha
                + "\n"
                + "author Test <test@test.invalid> 1 +0000\n"
                + "committer Test <test@test.invalid> 1 +0000\n"
                + "\n"
                + "broken meta body -- parent does not exist in the local ODB\n")
            .getBytes(StandardCharsets.UTF_8);

    // sha1 in RevisionObjectData is informational; ApplyObject computes the real
    // id via ObjectInserter. The parent-existence check operates on the parent
    // SHA parsed from the commit body, which is what triggers
    // MissingParentObjectException here.
    RevisionObjectData commit =
        new RevisionObjectData(fakeParentSha, Constants.OBJ_COMMIT, commitBody);
    RevisionObjectData tree = new RevisionObjectData(treeSha, Constants.OBJ_TREE, new byte[] {});
    return new RevisionData(ImmutableList.of(), commit, tree, ImmutableList.of());
  }
}

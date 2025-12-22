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

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import static com.google.common.truth.Truth.assertThat;
import static com.google.gerrit.testing.GerritJUnit.assertThrows;

import com.gerritforge.gerrit.plugins.replication.pull.FetchRefSpec;
import com.gerritforge.gerrit.plugins.replication.pull.RevisionReader;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Bytes;
import com.google.gerrit.acceptance.LightweightPluginDaemonTest;
import com.google.gerrit.acceptance.PushOneCommit.Result;
import com.google.gerrit.acceptance.SkipProjectClone;
import com.google.gerrit.acceptance.TestPlugin;
import com.google.gerrit.acceptance.UseLocalDisk;
import com.google.gerrit.acceptance.testsuite.project.ProjectOperations;
import com.google.gerrit.entities.Change;
import com.google.gerrit.entities.Patch;
import com.google.gerrit.entities.PatchSet;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.Project.NameKey;
import com.google.gerrit.entities.RefNames;
import com.google.gerrit.extensions.api.changes.ReviewInput;
import com.google.gerrit.extensions.api.changes.ReviewInput.CommentInput;
import com.google.gerrit.extensions.client.Comment;
import com.google.gerrit.extensions.config.FactoryModule;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.inject.Inject;
import com.google.inject.Scopes;
import com.googlesource.gerrit.plugins.replication.FileConfigResource;
import com.googlesource.gerrit.plugins.replication.ReplicationConfigImpl;
import com.googlesource.gerrit.plugins.replication.api.ConfigResource;
import com.googlesource.gerrit.plugins.replication.api.ReplicationConfig;
import com.googlesource.gerrit.plugins.replication.api.ReplicationConfigOverrides;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.jgit.junit.TestRepository;
import org.eclipse.jgit.lib.Repository;
import org.junit.Before;
import org.junit.Test;

@SkipProjectClone
@UseLocalDisk
@TestPlugin(
    name = "pull-replication",
    sysModule = "com.gerritforge.gerrit.plugins.replication.pull.fetch.ApplyObjectIT$TestModule")
public class ApplyObjectIT extends LightweightPluginDaemonTest {
  private static final String TEST_REPLICATION_SUFFIX = "suffix1";

  @Inject private ProjectOperations projectOperations;
  @Inject ApplyObject objectUnderTest;
  RevisionReader reader;

  @Before
  public void setup() {
    reader = plugin.getSysInjector().getInstance(RevisionReader.class);
  }

  @Test
  public void shouldApplyRefMetaObject() throws Exception {
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    testRepo = cloneProject(createTestProject(testRepoProjectName));

    Result pushResult = createChange();
    Change.Id changeId = pushResult.getChange().getId();
    String refName = RefNames.changeMetaRef(changeId);
    String patchSetRefName = RefNames.patchSetRef(PatchSet.id(changeId, 1));

    FetchRefSpec refSpec = FetchRefSpec.fromRef(refName);
    Optional<RevisionData> revisionData;
    NameKey testRepoKey = Project.nameKey(testRepoProjectName);

    try (Repository repo = repoManager.openRepository(testRepoKey)) {
      revisionData = reader.read(testRepoKey, repo.exactRef(refName).getObjectId(), refName, 0);
      objectUnderTest.apply(project, FetchRefSpec.fromRef(patchSetRefName), toArray(revisionData));
      objectUnderTest.apply(project, refSpec, toArray(revisionData));
    }

    try (Repository repo = repoManager.openRepository(project);
        TestRepository<Repository> testRepo = new TestRepository<>(repo); ) {
      Optional<RevisionData> newRevisionData =
          reader.read(project, repo.exactRef(refName).getObjectId(), refName, 0);
      compareObjects(revisionData.get(), newRevisionData);
      testRepo.fsck();
    }
  }

  @Test
  public void shouldApplyRefSequencesChanges() throws Exception {
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    testRepo = cloneProject(createTestProject(testRepoProjectName));

    createChange();
    String seqChangesRef = RefNames.REFS_SEQUENCES + "changes";

    Optional<RevisionData> revisionData = reader.read(allProjects, seqChangesRef, 0);

    FetchRefSpec refSpec = FetchRefSpec.fromRef(seqChangesRef);
    objectUnderTest.apply(project, refSpec, toArray(revisionData));
    try (Repository repo = repoManager.openRepository(project);
        TestRepository<Repository> testRepo = new TestRepository<>(repo); ) {

      Optional<RevisionData> newRevisionData =
          reader.read(project, repo.exactRef(seqChangesRef).getObjectId(), seqChangesRef, 0);
      compareObjects(revisionData.get(), newRevisionData);
      testRepo.fsck();
    }
  }

  @Test
  public void shouldApplyRefMetaObjectWithComments() throws Exception {
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    testRepo = cloneProject(createTestProject(testRepoProjectName));

    Result pushResult = createChange();
    Change.Id changeId = pushResult.getChange().getId();
    String patchSetRefname = RefNames.patchSetRef(PatchSet.id(changeId, 1));
    String refName = RefNames.changeMetaRef(changeId);
    FetchRefSpec refSpec = FetchRefSpec.fromRef(refName);

    NameKey testRepoKey = Project.nameKey(testRepoProjectName);
    try (Repository repo = repoManager.openRepository(testRepoKey)) {
      Optional<RevisionData> revisionData =
          reader.read(testRepoKey, repo.exactRef(refName).getObjectId(), refName, 0);
      objectUnderTest.apply(project, FetchRefSpec.fromRef(patchSetRefname), toArray(revisionData));
      objectUnderTest.apply(project, refSpec, toArray(revisionData));
    }

    ReviewInput reviewInput = new ReviewInput();
    CommentInput comment = createCommentInput(1, 0, 1, 1, "Test comment");
    reviewInput.comments = ImmutableMap.of(Patch.COMMIT_MSG, ImmutableList.of(comment));
    gApi.changes().id(changeId.get()).current().review(reviewInput);

    try (Repository repo = repoManager.openRepository(project);
        TestRepository<Repository> testRepo = new TestRepository<>(repo)) {
      Optional<RevisionData> revisionDataWithComment =
          reader.read(testRepoKey, repo.exactRef(refName).getObjectId(), refName, 0);

      objectUnderTest.apply(project, refSpec, toArray(revisionDataWithComment));

      Optional<RevisionData> newRevisionData =
          reader.read(project, repo.exactRef(refName).getObjectId(), refName, 0);

      compareObjects(revisionDataWithComment.get(), newRevisionData);

      testRepo.fsck();
    }
  }

  @Test
  public void shouldThrowExceptionWhenParentCommitObjectIsMissing() throws Exception {
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    NameKey createTestProject = createTestProject(testRepoProjectName);
    try (Repository repo = repoManager.openRepository(createTestProject)) {
      testRepo = cloneProject(createTestProject);

      Result pushResult = createChange();
      Change.Id changeId = pushResult.getChange().getId();
      String refName = RefNames.changeMetaRef(changeId);

      CommentInput comment = createCommentInput(1, 0, 1, 1, "Test comment");
      ReviewInput reviewInput = new ReviewInput();
      reviewInput.comments = ImmutableMap.of(Patch.COMMIT_MSG, ImmutableList.of(comment));
      gApi.changes().id(changeId.get()).current().review(reviewInput);

      Optional<RevisionData> revisionData =
          reader.read(createTestProject, repo.exactRef(refName).getObjectId(), refName, 0);

      FetchRefSpec refSpec = FetchRefSpec.fromRef(refName);
      assertThrows(
          MissingParentObjectException.class,
          () -> objectUnderTest.apply(project, refSpec, toArray(revisionData)));
    }
  }

  @Test
  public void shouldThrowExceptionWhenPatchSetIsMissing() throws Exception {
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    NameKey createTestProject = createTestProject(testRepoProjectName);
    try (Repository repo = repoManager.openRepository(createTestProject)) {
      testRepo = cloneProject(createTestProject);

      Result pushResult = createChange();
      Change.Id changeId = pushResult.getChange().getId();
      String refName = RefNames.changeMetaRef(changeId);

      Optional<RevisionData> revisionData =
          reader.read(createTestProject, repo.exactRef(refName).getObjectId(), refName, 0);

      FetchRefSpec refSpec = FetchRefSpec.fromRef(refName);
      assertThrows(
          MissingLatestPatchSetException.class,
          () -> objectUnderTest.apply(project, refSpec, toArray(revisionData)));
    }
  }

  @Test
  public void shouldRejectBatchWhenRefAdvancedOutOfBandBeforeExecute() throws Exception {
    // Pin the TOCTOU window inside ApplyObject.applyBatch between BatchApplyObject.add
    // (which reads exactRef to snapshot oldObjectId) and BatchRefUpdate.execute (which
    // verifies the snapshot against the current ref state). If a concurrent writer
    // advances the ref between add and execute, JGit's BatchRefUpdate rejects with
    // LOCK_FAILURE (or REJECTED_OTHER_REASON when the batch is atomic). The contract
    // pinned here: out-of-band advance => batch fails => no ref state from the batch
    // lands.
    //
    // Single-threaded controlled race: invoke BatchApplyObject directly (package-private
    // helper, accessible from this test's package), drive add() to snapshot oldObjectId,
    // then advance master via a direct RefUpdate, then call execute. No latch needed —
    // the operations are sequential.
    String testRepoProjectName = project + TEST_REPLICATION_SUFFIX;
    NameKey p = createTestProject(testRepoProjectName);
    testRepo = cloneProject(p);

    org.eclipse.jgit.lib.ObjectId initialMaster;
    try (Repository repo = repoManager.openRepository(p)) {
      initialMaster = repo.exactRef("refs/heads/master").getObjectId();
    }

    // Ship initialMaster's RevisionData as the master update. The new tip equals the
    // current tip, so the ReceiveCommand becomes (initialMaster, initialMaster, master)
    // = NO_CHANGE on its own. The interesting bit is the out-of-band advance below: it
    // moves master to a different OID, making the snapshotted cmd.oldId stale.
    Optional<RevisionData> masterRevOpt;
    try (Repository repo = repoManager.openRepository(p)) {
      masterRevOpt = reader.read(p, initialMaster, "refs/heads/master", 0);
    }
    RevisionData masterRev = masterRevOpt.orElseThrow();

    try (Repository git = repoManager.openRepository(p);
        BatchApplyObject batch = BatchApplyObject.create(git)) {
      // 1) add() snapshots oldObjectId = initialMaster.
      batch.add(
          p,
          new org.eclipse.jgit.transport.RefSpec("refs/heads/master"),
          new RevisionData[] {masterRev});

      // 2) Out-of-band advance master via a direct RefUpdate. This moves the real ref
      //    to a new OID while the batch still holds the stale snapshot.
      org.eclipse.jgit.lib.ObjectId advancedOid;
      try (org.eclipse.jgit.lib.ObjectInserter ins = git.newObjectInserter()) {
        // Insert a brand-new dummy commit to advance master to.
        org.eclipse.jgit.lib.CommitBuilder cb = new org.eclipse.jgit.lib.CommitBuilder();
        cb.setTreeId(git.parseCommit(initialMaster).getTree().getId());
        cb.setParentId(initialMaster);
        cb.setAuthor(
            new org.eclipse.jgit.lib.PersonIdent("Concurrent Writer", "writer@test.invalid"));
        cb.setCommitter(
            new org.eclipse.jgit.lib.PersonIdent("Concurrent Writer", "writer@test.invalid"));
        cb.setMessage("Out-of-band advance during batch-apply-object race test");
        advancedOid = ins.insert(cb);
        ins.flush();
      }
      org.eclipse.jgit.lib.RefUpdate ru = git.updateRef("refs/heads/master");
      ru.setNewObjectId(advancedOid);
      ru.setExpectedOldObjectId(initialMaster);
      org.eclipse.jgit.lib.RefUpdate.Result outOfBandResult = ru.update();
      assertThat(outOfBandResult)
          .isAnyOf(
              org.eclipse.jgit.lib.RefUpdate.Result.FAST_FORWARD,
              org.eclipse.jgit.lib.RefUpdate.Result.NEW);

      // 3) Execute the batch — JGit's BatchRefUpdate compares each command's
      //    snapshotted oldId to the current ref. master is now at advancedOid !=
      //    initialMaster, so the command is rejected.
      try (org.eclipse.jgit.revwalk.RevWalk rw = new org.eclipse.jgit.revwalk.RevWalk(git)) {
        batch.getBatchRefUpdate().execute(rw, org.eclipse.jgit.lib.NullProgressMonitor.INSTANCE);
      }

      BatchRefUpdateState state = new BatchRefUpdateState(batch.getBatchRefUpdate());
      assertThat(state.isSuccessful()).isFalse();
      assertThat(state.getCommands().get(0).getResult())
          .isAnyOf(
              org.eclipse.jgit.transport.ReceiveCommand.Result.LOCK_FAILURE,
              org.eclipse.jgit.transport.ReceiveCommand.Result.REJECTED_OTHER_REASON);

      // master is at advancedOid (the out-of-band write), not the batch's intended OID.
      try (Repository repo = repoManager.openRepository(p)) {
        assertThat(repo.exactRef("refs/heads/master").getObjectId()).isEqualTo(advancedOid);
      }
    }
  }

  @Test
  public void shouldThrowResourceNotFoundWhenProjectRepositoryMissing() throws Exception {
    // Pin the RepositoryNotFoundException -> ResourceNotFoundException translation
    // in ApplyObject.applyBatch. Without it, the underlying JGit
    // RepositoryNotFoundException would surface as a generic IOException and the
    // REST layer would respond 500 instead of 404.
    NameKey missingProject = Project.nameKey("project-that-does-not-exist");
    org.eclipse.jgit.transport.RefSpec refSpec =
        new org.eclipse.jgit.transport.RefSpec("refs/heads/master");
    RevisionData emptyRev =
        new RevisionData(
            ImmutableList.of(),
            new RevisionObjectData(
                "0".repeat(40), org.eclipse.jgit.lib.Constants.OBJ_COMMIT, new byte[] {}),
            new RevisionObjectData(
                "0".repeat(40), org.eclipse.jgit.lib.Constants.OBJ_TREE, new byte[] {}),
            ImmutableList.of());

    assertThrows(
        com.google.gerrit.extensions.restapi.ResourceNotFoundException.class,
        () -> objectUnderTest.apply(missingProject, refSpec, new RevisionData[] {emptyRev}));
  }

  private void compareObjects(RevisionData expected, Optional<RevisionData> actualOption) {
    assertThat(actualOption.isPresent()).isTrue();
    RevisionData actual = actualOption.get();
    compareContent(expected.getCommitObject(), actual.getCommitObject());
    compareContent(expected.getTreeObject(), expected.getTreeObject());
    List<List<Byte>> actualBlobs =
        actual.getBlobs().stream()
            .map(revision -> Bytes.asList(revision.getContent()))
            .collect(Collectors.toList());
    List<List<Byte>> expectedBlobs =
        expected.getBlobs().stream()
            .map(revision -> Bytes.asList(revision.getContent()))
            .collect(Collectors.toList());
    assertThat(actualBlobs).containsExactlyElementsIn(expectedBlobs);
  }

  private void compareContent(RevisionObjectData expected, RevisionObjectData actual) {
    if (expected == actual) {
      return;
    }
    assertThat(actual.getType()).isEqualTo(expected.getType());
    assertThat(Bytes.asList(actual.getContent()))
        .containsExactlyElementsIn(Bytes.asList(expected.getContent()))
        .inOrder();
  }

  private CommentInput createCommentInput(
      int startLine, int startCharacter, int endLine, int endCharacter, String message) {
    CommentInput comment = new CommentInput();
    comment.range = new Comment.Range();
    comment.range.startLine = startLine;
    comment.range.startCharacter = startCharacter;
    comment.range.endLine = endLine;
    comment.range.endCharacter = endCharacter;
    comment.message = message;
    comment.path = Patch.COMMIT_MSG;
    return comment;
  }

  private Project.NameKey createTestProject(String name) throws Exception {
    return projectOperations.newProject().name(name).create();
  }

  @SuppressWarnings("unused")
  private static class TestModule extends FactoryModule {
    @Override
    protected void configure() {
      DynamicItem.itemOf(binder(), ReplicationConfigOverrides.class);
      bind(ConfigResource.class).to(FileConfigResource.class);
      bind(ReplicationConfig.class).to(ReplicationConfigImpl.class);
      bind(RevisionReader.class).in(Scopes.SINGLETON);
      bind(ApplyObject.class);
    }
  }

  private RevisionData[] toArray(Optional<RevisionData> optional) {
    ImmutableList.Builder<RevisionData> listBuilder = ImmutableList.builder();
    optional.ifPresent(listBuilder::add);
    return listBuilder.build().toArray(new RevisionData[1]);
  }
}

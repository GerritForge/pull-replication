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

package com.gerritforge.gerrit.plugins.replication.pull.api;

import static com.google.common.truth.Truth.assertThat;
import static com.google.gerrit.testing.GerritJUnit.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectMetrics;
import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectsCacheKey;
import com.gerritforge.gerrit.plugins.replication.pull.FetchRefReplicatedEvent;
import com.gerritforge.gerrit.plugins.replication.pull.PullReplicationStateLogger;
import com.gerritforge.gerrit.plugins.replication.pull.ReplicationState.RefFetchResult;
import com.gerritforge.gerrit.plugins.replication.pull.Source;
import com.gerritforge.gerrit.plugins.replication.pull.SourcesCollection;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchApplyObject;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchRefUpdateState;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.Project.NameKey;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.metrics.Timer1;
import com.google.gerrit.server.events.Event;
import com.google.gerrit.server.events.EventDispatcher;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.eclipse.jgit.lib.BatchRefUpdate;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.URIish;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchApplyObjectCommandTest {
  private static final String TEST_SOURCE_LABEL = "test-source-label";
  private static final String TEST_REF_NAME = "refs/changes/01/1/1";
  private static final String TEST_REF_NAME_2 = "refs/changes/02/2/1";
  private static final NameKey TEST_PROJECT_NAME = Project.nameKey("test-project");
  private static URIish TEST_REMOTE_URI;
  private static final long TEST_EVENT_TIMESTAMP = 1L;

  private final String sampleCommitObjectId = "9f8d52853089a3cf00c02ff7bd0817bd4353a95a";
  private final String sampleTreeObjectId = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
  private final String sampleCommitObjectId2 = "9f8d52853089a3cf00c02ff7bd0817bd4353a95b";
  private final String sampleTreeObjectId2 = "4b825dc642cb6eb9a060e54bf8d69288fbee4905";

  @Mock private PullReplicationStateLogger fetchStateLog;
  @Mock private BatchApplyObject batchApplyObject;
  @Mock private ApplyObjectMetrics metrics;
  @Mock private DynamicItem<EventDispatcher> eventDispatcherDataItem;
  @Mock private EventDispatcher eventDispatcher;
  @Mock private Timer1.Context<String> timerContext;
  @Mock private SourcesCollection sourceCollection;
  @Mock private Source source;
  @Mock private BatchRefUpdate bru;
  @Captor private ArgumentCaptor<Event> eventCaptor;

  private Cache<ApplyObjectsCacheKey, Long> cache;
  private BatchApplyObjectCommand objectUnderTest;

  @Before
  public void setup() throws Exception {
    cache = CacheBuilder.newBuilder().build();
    TEST_REMOTE_URI = new URIish("git://some.remote.uri");
    when(eventDispatcherDataItem.get()).thenReturn(eventDispatcher);
    when(metrics.start(anyString())).thenReturn(timerContext);
    when(timerContext.stop()).thenReturn(100L);
    when(sourceCollection.getByRemoteName(TEST_SOURCE_LABEL)).thenReturn(Optional.of(source));
    when(source.getURI(TEST_PROJECT_NAME)).thenReturn(TEST_REMOTE_URI);

    objectUnderTest =
        new BatchApplyObjectCommand(
            fetchStateLog,
            batchApplyObject,
            metrics,
            eventDispatcherDataItem,
            sourceCollection,
            cache);
  }

  @Test
  public void shouldPostOneEventPerReceiveCommandInInputOrder() throws Exception {
    ReceiveCommand cmd1 = receiveCommandForRef(TEST_REF_NAME, ReceiveCommand.Result.OK);
    ReceiveCommand cmd2 = receiveCommandForRef(TEST_REF_NAME_2, ReceiveCommand.Result.OK);
    when(bru.getCommands()).thenReturn(List.of(cmd1, cmd2));
    when(batchApplyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));

    RevisionData rev1 = createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);
    RevisionData rev2 = createSampleRevisionData(sampleCommitObjectId2, sampleTreeObjectId2);

    objectUnderTest.batchApplyObjects(
        "Batch",
        TEST_PROJECT_NAME,
        List.of(TEST_REF_NAME, TEST_REF_NAME_2),
        List.of(new RevisionData[] {rev1}, new RevisionData[] {rev2}),
        TEST_SOURCE_LABEL,
        TEST_EVENT_TIMESTAMP);

    verify(eventDispatcher, times(2)).postEvent(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertThat(events).hasSize(2);
    assertThat(events.get(0)).isInstanceOf(FetchRefReplicatedEvent.class);
    assertThat(events.get(1)).isInstanceOf(FetchRefReplicatedEvent.class);
    assertThat(
            events.stream()
                .map(e -> ((FetchRefReplicatedEvent) e).getRefName())
                .toList())
        .containsExactly(TEST_REF_NAME, TEST_REF_NAME_2)
        .inOrder();
    assertThat(((FetchRefReplicatedEvent) events.get(0)).getStatus())
        .isEqualTo(RefFetchResult.SUCCEEDED.toString());
    assertThat(((FetchRefReplicatedEvent) events.get(1)).getStatus())
        .isEqualTo(RefFetchResult.SUCCEEDED.toString());
  }

  @Test
  public void shouldPostOneFailedEventPerReceiveCommandOnBatchFailure() throws Exception {
    ReceiveCommand cmd1 =
        receiveCommandForRef(TEST_REF_NAME, ReceiveCommand.Result.REJECTED_NONFASTFORWARD);
    ReceiveCommand cmd2 =
        receiveCommandForRef(TEST_REF_NAME_2, ReceiveCommand.Result.REJECTED_OTHER_REASON);
    when(bru.getCommands()).thenReturn(List.of(cmd1, cmd2));
    when(batchApplyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));

    RevisionData rev1 = createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);
    RevisionData rev2 = createSampleRevisionData(sampleCommitObjectId2, sampleTreeObjectId2);

    assertThrows(
        BatchRefUpdateException.class,
        () ->
            objectUnderTest.batchApplyObjects(
                "Batch",
                TEST_PROJECT_NAME,
                List.of(TEST_REF_NAME, TEST_REF_NAME_2),
                List.of(new RevisionData[] {rev1}, new RevisionData[] {rev2}),
                TEST_SOURCE_LABEL,
                TEST_EVENT_TIMESTAMP));

    verify(eventDispatcher, times(2)).postEvent(eventCaptor.capture());
    List<Event> events = eventCaptor.getAllValues();
    assertThat(events).hasSize(2);
    assertThat(((FetchRefReplicatedEvent) events.get(0)).getStatus())
        .isEqualTo(RefFetchResult.FAILED.toString());
    assertThat(((FetchRefReplicatedEvent) events.get(1)).getStatus())
        .isEqualTo(RefFetchResult.FAILED.toString());
    assertThat(
            events.stream()
                .map(e -> ((FetchRefReplicatedEvent) e).getRefName())
                .toList())
        .containsExactly(TEST_REF_NAME, TEST_REF_NAME_2)
        .inOrder();
  }

  @Test
  public void decodeResultMapsEveryReceiveCommandResult() {
    ObjectId oldOid = ObjectId.fromString("1111111111111111111111111111111111111111");
    ObjectId newOid = ObjectId.fromString("2222222222222222222222222222222222222222");

    // OK / CREATE -> NEW
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.OK, ReceiveCommand.Type.CREATE, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.NEW);

    // OK / UPDATE -> FAST_FORWARD
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.OK, ReceiveCommand.Type.UPDATE, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.FAST_FORWARD);

    // OK / UPDATE_NONFASTFORWARD -> FORCED
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(
                    ReceiveCommand.Result.OK,
                    ReceiveCommand.Type.UPDATE_NONFASTFORWARD,
                    oldOid,
                    newOid)))
        .isEqualTo(RefUpdate.Result.FORCED);

    // OK / DELETE -> FORCED
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.OK, ReceiveCommand.Type.DELETE, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.FORCED);

    // OK with oldId == newId -> NO_CHANGE (type ignored when ids match)
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.OK, ReceiveCommand.Type.UPDATE, oldOid, oldOid)))
        .isEqualTo(RefUpdate.Result.NO_CHANGE);

    // REJECTED_* -> REJECTED
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_NOCREATE, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED);
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_NODELETE, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED);
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_NONFASTFORWARD, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED);
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_OTHER_REASON, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED);

    // REJECTED_CURRENT_BRANCH
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_CURRENT_BRANCH, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED_CURRENT_BRANCH);

    // REJECTED_MISSING_OBJECT
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.REJECTED_MISSING_OBJECT, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.REJECTED_MISSING_OBJECT);

    // LOCK_FAILURE
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.LOCK_FAILURE, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.LOCK_FAILURE);

    // NOT_ATTEMPTED
    assertThat(
            BatchApplyObjectCommand.decodeResult(
                mockCmd(ReceiveCommand.Result.NOT_ATTEMPTED, null, oldOid, newOid)))
        .isEqualTo(RefUpdate.Result.NOT_ATTEMPTED);
  }

  private static ReceiveCommand receiveCommandForRef(
      String refName, ReceiveCommand.Result result) {
    ReceiveCommand cmd = mock(ReceiveCommand.class);
    when(cmd.getRefName()).thenReturn(refName);
    when(cmd.getResult()).thenReturn(result);
    when(cmd.getOldId()).thenReturn(ObjectId.zeroId());
    when(cmd.getNewId()).thenReturn(ObjectId.zeroId());
    return cmd;
  }

  private static ReceiveCommand mockCmd(
      ReceiveCommand.Result result, ReceiveCommand.Type type, ObjectId oldId, ObjectId newId) {
    ReceiveCommand cmd = mock(ReceiveCommand.class);
    when(cmd.getResult()).thenReturn(result);
    if (type != null) {
      when(cmd.getType()).thenReturn(type);
    }
    when(cmd.getOldId()).thenReturn(oldId);
    when(cmd.getNewId()).thenReturn(newId);
    return cmd;
  }

  private RevisionData createSampleRevisionData(String commitObjectId, String treeObjectId) {
    RevisionObjectData commitData =
        new RevisionObjectData(commitObjectId, Constants.OBJ_COMMIT, new byte[] {});
    RevisionObjectData treeData =
        new RevisionObjectData(treeObjectId, Constants.OBJ_TREE, new byte[] {});
    return new RevisionData(Collections.emptyList(), commitData, treeData, Lists.newArrayList());
  }
}

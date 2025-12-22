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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectMetrics;
import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectsCacheKey;
import com.gerritforge.gerrit.plugins.replication.pull.FetchRefReplicatedEvent;
import com.gerritforge.gerrit.plugins.replication.pull.PullReplicationStateLogger;
import com.gerritforge.gerrit.plugins.replication.pull.ReplicationState;
import com.gerritforge.gerrit.plugins.replication.pull.Source;
import com.gerritforge.gerrit.plugins.replication.pull.SourcesCollection;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.ApplyObject;
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
import java.util.stream.Collectors;
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
public class ApplyObjectCommandTest {
  private static final String TEST_SOURCE_LABEL = "test-source-label";
  private static final String TEST_REF_NAME = "refs/changes/01/1/1";
  private static final NameKey TEST_PROJECT_NAME = Project.nameKey("test-project");
  private static URIish TEST_REMOTE_URI;
  private static final long TEST_EVENT_TIMESTAMP = 1L;

  private String sampleCommitObjectId = "9f8d52853089a3cf00c02ff7bd0817bd4353a95a";
  private String sampleTreeObjectId = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
  private String sampleBlobObjectId = "b5d7bcf1d1c5b0f0726d10a16c8315f06f900bfb";
  private String sampleCommitObjectId2 = "9f8d52853089a3cf00c02ff7bd0817bd4353a95b";
  private String sampleTreeObjectId2 = "4b825dc642cb6eb9a060e54bf8d69288fbee4905";

  @Mock private PullReplicationStateLogger fetchStateLog;
  @Mock private ApplyObject applyObject;
  @Mock private ApplyObjectMetrics metrics;
  @Mock private DynamicItem<EventDispatcher> eventDispatcherDataItem;
  @Mock private EventDispatcher eventDispatcher;
  @Mock private Timer1.Context<String> timetContext;
  @Mock private SourcesCollection sourceCollection;
  @Mock private Source source;
  @Mock private BatchRefUpdate bru;
  @Captor ArgumentCaptor<Event> eventCaptor;
  private Cache<ApplyObjectsCacheKey, Long> cache;

  private ApplyObjectCommand objectUnderTest;

  @Before
  public void setup() throws Exception {
    cache = CacheBuilder.newBuilder().build();
    when(bru.getCommands()).thenReturn(List.of(receiveCommand(ReceiveCommand.Result.OK)));
    TEST_REMOTE_URI = new URIish("git://some.remote.uri");
    when(eventDispatcherDataItem.get()).thenReturn(eventDispatcher);
    when(metrics.start(anyString())).thenReturn(timetContext);
    when(timetContext.stop()).thenReturn(100L);
    when(applyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));
    when(sourceCollection.getByRemoteName(TEST_SOURCE_LABEL)).thenReturn(Optional.of(source));
    when(source.getURI(TEST_PROJECT_NAME)).thenReturn(TEST_REMOTE_URI);

    objectUnderTest =
        new ApplyObjectCommand(
            fetchStateLog, applyObject, metrics, eventDispatcherDataItem, sourceCollection, cache);
  }

  @Test
  public void shouldSendEventWhenApplyObject() throws Exception {
    RevisionData sampleRevisionData =
        createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);
    objectUnderTest.applyObject(
        TEST_PROJECT_NAME,
        TEST_REF_NAME,
        sampleRevisionData,
        TEST_SOURCE_LABEL,
        TEST_EVENT_TIMESTAMP);

    verify(eventDispatcher).postEvent(eventCaptor.capture());
    Event sentEvent = eventCaptor.getValue();
    assertThat(sentEvent).isInstanceOf(FetchRefReplicatedEvent.class);
    FetchRefReplicatedEvent fetchEvent = (FetchRefReplicatedEvent) sentEvent;
    assertThat(fetchEvent.getProjectNameKey()).isEqualTo(TEST_PROJECT_NAME);
    assertThat(fetchEvent.getRefName()).isEqualTo(TEST_REF_NAME);
    assertThat(fetchEvent.targetUri).isEqualTo(TEST_REMOTE_URI.toASCIIString());
  }

  @Test
  public void shouldInsertIntoApplyObjectsCacheWhenApplyObjectIsSuccessful() throws Exception {
    RevisionData sampleRevisionData =
        createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);
    RevisionData sampleRevisionData2 =
        createSampleRevisionData(sampleCommitObjectId2, sampleTreeObjectId2);
    objectUnderTest.applyObjects(
        TEST_PROJECT_NAME,
        TEST_REF_NAME,
        new RevisionData[] {sampleRevisionData, sampleRevisionData2},
        TEST_SOURCE_LABEL,
        TEST_EVENT_TIMESTAMP);

    assertThat(
            cache.getIfPresent(
                ApplyObjectsCacheKey.create(
                    sampleRevisionData.getCommitObject().getSha1(),
                    TEST_REF_NAME,
                    TEST_PROJECT_NAME.get())))
        .isEqualTo(TEST_EVENT_TIMESTAMP);
    assertThat(
            cache.getIfPresent(
                ApplyObjectsCacheKey.create(
                    sampleRevisionData2.getCommitObject().getSha1(),
                    TEST_REF_NAME,
                    TEST_PROJECT_NAME.get())))
        .isEqualTo(TEST_EVENT_TIMESTAMP);
  }

  @Test(expected = BatchRefUpdateException.class)
  public void shouldNotInsertIntoApplyObjectsCacheWhenApplyObjectIsFailure() throws Exception {
    RevisionData sampleRevisionData =
        createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);
    when(bru.getCommands())
        .thenReturn(List.of(receiveCommand(ReceiveCommand.Result.REJECTED_MISSING_OBJECT)));
    when(applyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));
    objectUnderTest.applyObject(
        TEST_PROJECT_NAME,
        TEST_REF_NAME,
        sampleRevisionData,
        TEST_SOURCE_LABEL,
        TEST_EVENT_TIMESTAMP);

    assertThat(
            cache.getIfPresent(
                ApplyObjectsCacheKey.create(
                    sampleRevisionData.getCommitObject().getSha1(),
                    TEST_REF_NAME,
                    TEST_PROJECT_NAME.get())))
        .isNull();
  }

  @Test
  public void shouldPostOneEventPerReceiveCommandInInputOrder() throws Exception {
    // Pin two contracts at once:
    //  1. Cardinality: one FetchRefReplicatedEvent per ReceiveCommand.
    //  2. Ordering: events match the BatchRefUpdate.getCommands() input order.
    // Downstream consumers (multi-site forwarders, log readers) rely on the
    // ordering to attribute per-ref outcomes to specific commands.
    RevisionData sampleRevisionData =
        createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);

    String firstRef = "refs/changes/01/1/1";
    String secondRef = "refs/changes/02/2/1";
    when(bru.getCommands())
        .thenReturn(
            List.of(
                receiveCommandForRef(firstRef, ReceiveCommand.Result.OK),
                receiveCommandForRef(secondRef, ReceiveCommand.Result.OK)));
    when(applyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));

    objectUnderTest.applyObject(
        TEST_PROJECT_NAME,
        TEST_REF_NAME,
        sampleRevisionData,
        TEST_SOURCE_LABEL,
        TEST_EVENT_TIMESTAMP);

    verify(eventDispatcher, times(2)).postEvent(eventCaptor.capture());
    List<String> refs =
        eventCaptor.getAllValues().stream()
            .filter(e -> e instanceof FetchRefReplicatedEvent)
            .map(e -> ((FetchRefReplicatedEvent) e).getRefName())
            .collect(Collectors.toList());
    assertThat(refs).containsExactly(TEST_REF_NAME, secondRef).inOrder();
  }

  @Test
  public void shouldPostOneFailedEventPerReceiveCommandOnBatchFailure() throws Exception {
    // Pin per-ref event cardinality and status on an atomic-batch failure.
    //
    // Contract: when the batch is rejected (BatchRefUpdateState.isSuccessful() == false),
    // ApplyObjectCommand still emits one FetchRefReplicatedEvent per ReceiveCommand —
    // each carrying RefFetchResult.FAILED — before throwing BatchRefUpdateException.
    // Operators rely on the per-ref FAILED events for observability; multi-site
    // forwarders rely on the cardinality matching the input batch.
    //
    // Regression pin: if the per-ref event emission is ever moved inside the
    // `if (isRefUpdateSuccessful)` block (zero events on failure), this test fires.
    RevisionData sampleRevisionData =
        createSampleRevisionData(sampleCommitObjectId, sampleTreeObjectId);

    String firstRef = "refs/changes/01/1/1";
    String secondRef = "refs/changes/02/2/1";
    when(bru.getCommands())
        .thenReturn(
            List.of(
                receiveCommandForRef(firstRef, ReceiveCommand.Result.REJECTED_NONFASTFORWARD),
                receiveCommandForRef(secondRef, ReceiveCommand.Result.REJECTED_OTHER_REASON)));
    when(applyObject.applyBatch(any(), any(), any())).thenReturn(new BatchRefUpdateState(bru));

    BatchRefUpdateException thrown =
        org.junit.Assert.assertThrows(
            BatchRefUpdateException.class,
            () ->
                objectUnderTest.applyObject(
                    TEST_PROJECT_NAME,
                    TEST_REF_NAME,
                    sampleRevisionData,
                    TEST_SOURCE_LABEL,
                    TEST_EVENT_TIMESTAMP));
    assertThat(thrown).isNotNull();

    verify(eventDispatcher, times(2)).postEvent(eventCaptor.capture());
    List<FetchRefReplicatedEvent> events =
        eventCaptor.getAllValues().stream()
            .filter(e -> e instanceof FetchRefReplicatedEvent)
            .map(e -> (FetchRefReplicatedEvent) e)
            .collect(Collectors.toList());
    assertThat(
            events.stream().map(FetchRefReplicatedEvent::getRefName).collect(Collectors.toList()))
        .containsExactly(TEST_REF_NAME, secondRef);
    assertThat(events.stream().map(FetchRefReplicatedEvent::getStatus).collect(Collectors.toList()))
        .containsExactly(
            ReplicationState.RefFetchResult.FAILED.toString(),
            ReplicationState.RefFetchResult.FAILED.toString());
  }

  @Test
  public void decodeResultMapsEveryReceiveCommandResult() {
    // Table test pinning the ReceiveCommand.Result -> RefUpdate.Result mapping
    // in ApplyObjectCommand.decodeResult. Every value of ReceiveCommand.Result is
    // exercised — adding a new value in jgit will (correctly) be a compile error
    // on the production switch and a missing-row error here.
    ObjectId oldId = ObjectId.zeroId();
    ObjectId newId = ObjectId.fromString(sampleCommitObjectId);

    // OK + CREATE: oldId zero, newId non-zero, auto-typed CREATE -> NEW
    ReceiveCommand create = new ReceiveCommand(oldId, newId, TEST_REF_NAME);
    create.setResult(ReceiveCommand.Result.OK);
    assertThat(ApplyObjectCommand.decodeResult(create)).isEqualTo(RefUpdate.Result.NEW);

    // OK + UPDATE: both non-zero, auto-typed UPDATE -> FAST_FORWARD
    ObjectId otherId = ObjectId.fromString(sampleCommitObjectId2);
    ReceiveCommand update = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
    update.setResult(ReceiveCommand.Result.OK);
    assertThat(ApplyObjectCommand.decodeResult(update)).isEqualTo(RefUpdate.Result.FAST_FORWARD);

    // OK + UPDATE_NONFASTFORWARD: forced update -> FORCED
    ReceiveCommand forced =
        new ReceiveCommand(
            otherId, newId, TEST_REF_NAME, ReceiveCommand.Type.UPDATE_NONFASTFORWARD);
    forced.setResult(ReceiveCommand.Result.OK);
    assertThat(ApplyObjectCommand.decodeResult(forced)).isEqualTo(RefUpdate.Result.FORCED);

    // OK + DELETE: newId zero -> default arm -> FORCED (no RefUpdate.Result.DELETE exists)
    ReceiveCommand delete = new ReceiveCommand(newId, oldId, TEST_REF_NAME);
    delete.setResult(ReceiveCommand.Result.OK);
    assertThat(ApplyObjectCommand.decodeResult(delete)).isEqualTo(RefUpdate.Result.FORCED);

    // OK + oldId == newId: NO_CHANGE regardless of type
    ReceiveCommand noChange = new ReceiveCommand(newId, newId, TEST_REF_NAME);
    noChange.setResult(ReceiveCommand.Result.OK);
    assertThat(ApplyObjectCommand.decodeResult(noChange)).isEqualTo(RefUpdate.Result.NO_CHANGE);

    // Rejection family -> REJECTED.
    for (ReceiveCommand.Result r :
        new ReceiveCommand.Result[] {
          ReceiveCommand.Result.REJECTED_NOCREATE,
          ReceiveCommand.Result.REJECTED_NODELETE,
          ReceiveCommand.Result.REJECTED_NONFASTFORWARD,
          ReceiveCommand.Result.REJECTED_OTHER_REASON
        }) {
      ReceiveCommand cmd = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
      cmd.setResult(r);
      assertThat(ApplyObjectCommand.decodeResult(cmd)).isEqualTo(RefUpdate.Result.REJECTED);
    }

    // REJECTED_CURRENT_BRANCH -> REJECTED_CURRENT_BRANCH.
    ReceiveCommand currentBranch = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
    currentBranch.setResult(ReceiveCommand.Result.REJECTED_CURRENT_BRANCH);
    assertThat(ApplyObjectCommand.decodeResult(currentBranch))
        .isEqualTo(RefUpdate.Result.REJECTED_CURRENT_BRANCH);

    // REJECTED_MISSING_OBJECT -> REJECTED_MISSING_OBJECT (was wrongly IO_FAILURE).
    ReceiveCommand missingObj = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
    missingObj.setResult(ReceiveCommand.Result.REJECTED_MISSING_OBJECT);
    assertThat(ApplyObjectCommand.decodeResult(missingObj))
        .isEqualTo(RefUpdate.Result.REJECTED_MISSING_OBJECT);

    // LOCK_FAILURE -> LOCK_FAILURE.
    ReceiveCommand lockFailure = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
    lockFailure.setResult(ReceiveCommand.Result.LOCK_FAILURE);
    assertThat(ApplyObjectCommand.decodeResult(lockFailure))
        .isEqualTo(RefUpdate.Result.LOCK_FAILURE);

    // NOT_ATTEMPTED -> NOT_ATTEMPTED (was wrongly collapsed into LOCK_FAILURE).
    ReceiveCommand notAttempted = new ReceiveCommand(otherId, newId, TEST_REF_NAME);
    notAttempted.setResult(ReceiveCommand.Result.NOT_ATTEMPTED);
    assertThat(ApplyObjectCommand.decodeResult(notAttempted))
        .isEqualTo(RefUpdate.Result.NOT_ATTEMPTED);
  }

  private RevisionData createSampleRevisionData(String commitObjectId, String treeObjectId) {
    RevisionObjectData commitData =
        new RevisionObjectData(commitObjectId, Constants.OBJ_COMMIT, new byte[] {});
    RevisionObjectData treeData =
        new RevisionObjectData(treeObjectId, Constants.OBJ_TREE, new byte[] {});
    return new RevisionData(Collections.emptyList(), commitData, treeData, Lists.newArrayList());
  }

  private ReceiveCommand receiveCommandForRef(String ref, ReceiveCommand.Result result) {
    ReceiveCommand receiveCommand =
        new ReceiveCommand(ObjectId.zeroId(), ObjectId.fromString(sampleCommitObjectId), ref);
    receiveCommand.setResult(result);
    return receiveCommand;
  }

  private ReceiveCommand receiveCommand(ReceiveCommand.Result result) {
    return receiveCommandForRef(TEST_REF_NAME, result);
  }
}

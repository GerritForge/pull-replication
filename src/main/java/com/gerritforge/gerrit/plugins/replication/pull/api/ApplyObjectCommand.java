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

import static com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectCacheModule.APPLY_OBJECTS_CACHE;
import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectMetrics;
import com.gerritforge.gerrit.plugins.replication.pull.ApplyObjectsCacheKey;
import com.gerritforge.gerrit.plugins.replication.pull.Context;
import com.gerritforge.gerrit.plugins.replication.pull.FetchRefReplicatedEvent;
import com.gerritforge.gerrit.plugins.replication.pull.PullReplicationStateLogger;
import com.gerritforge.gerrit.plugins.replication.pull.ReplicationState;
import com.gerritforge.gerrit.plugins.replication.pull.ReplicationState.RefFetchResult;
import com.gerritforge.gerrit.plugins.replication.pull.Source;
import com.gerritforge.gerrit.plugins.replication.pull.SourcesCollection;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.ApplyObject;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.BatchRefUpdateState;
import com.google.common.cache.Cache;
import com.google.common.flogger.FluentLogger;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.metrics.Timer1;
import com.google.gerrit.server.events.EventDispatcher;
import com.google.gerrit.server.permissions.PermissionBackendException;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.URIish;

public class ApplyObjectCommand {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final Cache<ApplyObjectsCacheKey, Long> refUpdatesSucceededCache;

  private final PullReplicationStateLogger fetchStateLog;
  private final ApplyObject applyObject;
  private final ApplyObjectMetrics metrics;
  private final DynamicItem<EventDispatcher> eventDispatcher;
  private final SourcesCollection sourcesCollection;

  @Inject
  public ApplyObjectCommand(
      PullReplicationStateLogger fetchStateLog,
      ApplyObject applyObject,
      ApplyObjectMetrics metrics,
      DynamicItem<EventDispatcher> eventDispatcher,
      SourcesCollection sourcesCollection,
      @Named(APPLY_OBJECTS_CACHE) Cache<ApplyObjectsCacheKey, Long> refUpdatesSucceededCache) {
    this.fetchStateLog = fetchStateLog;
    this.applyObject = applyObject;
    this.metrics = metrics;
    this.eventDispatcher = eventDispatcher;
    this.sourcesCollection = sourcesCollection;
    this.refUpdatesSucceededCache = refUpdatesSucceededCache;
  }

  public void applyObject(
      Project.NameKey name,
      String refName,
      RevisionData revisionsData,
      String sourceLabel,
      long eventCreatedOn)
      throws IOException,
          BatchRefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {
    batchApplyObjects(
        ApplyInvocationType.APPLY_OBJECT,
        name,
        Collections.singletonList(refName),
        Collections.singletonList(new RevisionData[] {revisionsData}),
        sourceLabel,
        eventCreatedOn);
  }

  public void applyObjects(
      Project.NameKey name,
      String refName,
      RevisionData[] revisionsData,
      String sourceLabel,
      long eventCreatedOn)
      throws IOException,
          BatchRefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {

    batchApplyObjects(
        ApplyInvocationType.APPLY_OBJECTS,
        name,
        Collections.singletonList(refName),
        Collections.singletonList(revisionsData),
        sourceLabel,
        eventCreatedOn);
  }

  public void batchApplyObjects(
      ApplyInvocationType invocationType,
      Project.NameKey name,
      List<String> refNames,
      List<RevisionData[]> revisionsDataList,
      String sourceLabel,
      long eventCreatedOn)
      throws IOException,
          BatchRefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {

    String refsForLog = String.join(",", refNames);
    String revisionsForLog =
        revisionsDataList.stream().map(Arrays::toString).collect(Collectors.joining(", "));

    repLog.info(
        "{} object from {} for {}:{} - {}",
        invocationType.logLabel(),
        sourceLabel,
        name,
        refsForLog,
        revisionsForLog);

    if (refNames.size() != revisionsDataList.size()) {
      throw new IllegalArgumentException(
          String.format(
              "%s Mismatched refs and revisions sizes: refs=%d, revisions=%d",
              invocationType.logLabel(), refNames.size(), revisionsDataList.size()));
    }

    Timer1.Context<String> context = metrics.start(sourceLabel);

    List<RefSpec> refSpecs = new ArrayList<>(refNames.size());
    for (String refName : refNames) {
      refSpecs.add(new RefSpec(refName));
    }

    BatchRefUpdateState refUpdateState = applyObject.applyBatch(name, refSpecs, revisionsDataList);
    boolean isRefUpdateSuccessful = refUpdateState.isSuccessful();

    if (isRefUpdateSuccessful) {
      for (int i = 0; i < refNames.size(); i++) {
        String refName = refNames.get(i);
        RevisionData[] revisionsData = revisionsDataList.get(i);

        for (RevisionData revisionData : revisionsData) {
          RevisionObjectData commitObj = revisionData.getCommitObject();
          List<RevisionObjectData> blobs = revisionData.getBlobs();

          if (commitObj != null) {
            refUpdatesSucceededCache.put(
                ApplyObjectsCacheKey.create(commitObj.getSha1(), refName, name.get()),
                eventCreatedOn);
          } else if (blobs != null) {
            for (RevisionObjectData blob : blobs) {
              refUpdatesSucceededCache.put(
                  ApplyObjectsCacheKey.create(blob.getSha1(), refName, name.get()), eventCreatedOn);
            }
          }
        }
      }
    }

    long elapsed = NANOSECONDS.toMillis(context.stop());

    try {
      Context.setLocalEvent(true);
      Source source =
          sourcesCollection
              .getByRemoteName(sourceLabel)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          String.format("Could not find URI for %s remote", sourceLabel)));
      String project = name.get();
      URIish sourceURI = source.getURI(name);
      RefFetchResult overallFetchStatus = getStatus(refUpdateState);

      for (ReceiveCommand receiveCommand : refUpdateState.getCommands()) {
        eventDispatcher
            .get()
            .postEvent(
                new FetchRefReplicatedEvent(
                    project,
                    receiveCommand.getRefName(),
                    sourceURI,
                    overallFetchStatus,
                    decodeResult(receiveCommand)));
      }
    } catch (PermissionBackendException | IllegalStateException e) {
      logger.atSevere().withCause(e).log(
          "Cannot post event for refs '%s', project %s", refsForLog, name);
    } finally {
      Context.unsetLocalEvent();
    }
    if (!isRefUpdateSuccessful) {
      String message =
          String.format(
              "RefUpdate failed for: sourceLabel=%s, project=%s. %s",
              sourceLabel, name, refUpdateState.toLogLine());
      fetchStateLog.error(message);
      throw new BatchRefUpdateException(refUpdateState, message);
    }

    repLog.info(
        "{} from {} for project {}, refs {} completed in {}ms",
        invocationType.logLabel(),
        sourceLabel,
        name,
        refsForLog,
        elapsed);
  }

  private RefFetchResult getStatus(BatchRefUpdateState refUpdateState) {
    return refUpdateState.isSuccessful()
        ? ReplicationState.RefFetchResult.SUCCEEDED
        : ReplicationState.RefFetchResult.FAILED;
  }

  private static RefUpdate.Result decodeResult(ReceiveCommand receiveCommand) {
    return switch (receiveCommand.getResult()) {
      case OK -> {
        if (AnyObjectId.isEqual(receiveCommand.getOldId(), receiveCommand.getNewId()))
          yield RefUpdate.Result.NO_CHANGE;
        yield switch (receiveCommand.getType()) {
          case CREATE -> RefUpdate.Result.NEW;
          case UPDATE -> RefUpdate.Result.FAST_FORWARD;
          default -> RefUpdate.Result.FORCED;
        };
      }
      case REJECTED_NOCREATE, REJECTED_NODELETE, REJECTED_NONFASTFORWARD ->
          RefUpdate.Result.REJECTED;
      case REJECTED_CURRENT_BRANCH -> RefUpdate.Result.REJECTED_CURRENT_BRANCH;
      case REJECTED_MISSING_OBJECT -> RefUpdate.Result.IO_FAILURE;
      default -> RefUpdate.Result.LOCK_FAILURE;
    };
  }

  public enum ApplyInvocationType {
    APPLY_OBJECT("Apply object"),
    APPLY_OBJECTS("Apply objects"),
    BATCH_APPLY_OBJECT("Batch Apply object");

    private final String logLabel;

    ApplyInvocationType(String logLabel) {
      this.logLabel = logLabel;
    }

    public String logLabel() {
      return logLabel;
    }
  }
}

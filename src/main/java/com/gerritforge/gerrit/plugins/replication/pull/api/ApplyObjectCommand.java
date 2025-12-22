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
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RefUpdateException;
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
import java.util.Arrays;
import java.util.List;
import org.eclipse.jgit.lib.AnyObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.transport.ReceiveCommand;
import org.eclipse.jgit.transport.RefSpec;

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
          RefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {
    applyObjects(name, refName, new RevisionData[] {revisionsData}, sourceLabel, eventCreatedOn);
  }

  public void applyObjects(
      Project.NameKey name,
      String refName,
      RevisionData[] revisionsData,
      String sourceLabel,
      long eventCreatedOn)
      throws IOException,
          RefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {

    repLog.info(
        "Apply object from {} for {}:{} - {}",
        sourceLabel,
        name,
        refName,
        Arrays.toString(revisionsData));
    Timer1.Context<String> context = metrics.start(sourceLabel);

    BatchRefUpdateState refUpdateState =
        applyObject.apply(name, new RefSpec(refName), revisionsData);
    boolean isRefUpdateSuccessful = refUpdateState.isSuccessful();

    ReceiveCommand singleCmd = refUpdateState.getCommands().getFirst();

    if (isRefUpdateSuccessful) {
      for (RevisionData revisionData : revisionsData) {
        RevisionObjectData commitObj = revisionData.getCommitObject();
        List<RevisionObjectData> blobs = revisionData.getBlobs();

        if (commitObj != null) {
          refUpdatesSucceededCache.put(
              ApplyObjectsCacheKey.create(
                  revisionData.getCommitObject().getSha1(), refName, name.get()),
              eventCreatedOn);
        } else if (blobs != null) {
          for (RevisionObjectData blob : blobs) {
            refUpdatesSucceededCache.put(
                ApplyObjectsCacheKey.create(blob.getSha1(), refName, name.get()), eventCreatedOn);
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
      eventDispatcher
          .get()
          .postEvent(
              new FetchRefReplicatedEvent(
                  name.get(),
                  refName,
                  source.getURI(name),
                  getStatus(refUpdateState),
                  decodeResult(singleCmd)));
    } catch (PermissionBackendException | IllegalStateException e) {
      logger.atSevere().withCause(e).log(
          "Cannot post event for ref '%s', project %s", refName, name);
    } finally {
      Context.unsetLocalEvent();
    }

    if (!isRefUpdateSuccessful) {
      String message =
          String.format(
              "RefUpdate failed with result %s for: sourceLabel=%s, project=%s, refName=%s",
              decodeResult(singleCmd).name(), sourceLabel, name, refName);
      fetchStateLog.error(message);
      throw new RefUpdateException(decodeResult(singleCmd), message);
    }
    repLog.info(
        "Apply object from {} for project {}, ref name {} completed in {}ms",
        sourceLabel,
        name,
        refName,
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
}

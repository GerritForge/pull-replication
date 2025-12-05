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
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionObjectData;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.ApplyObject;
import com.gerritforge.gerrit.plugins.replication.pull.fetch.RefUpdateState;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.stream.Collectors;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.transport.RefSpec;

public class ApplyObjectCommand {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();
  private final Cache<ApplyObjectsCacheKey, Long> refUpdatesSucceededCache;
  private static final Set<RefUpdate.Result> SUCCESSFUL_RESULTS =
      ImmutableSet.of(
          RefUpdate.Result.NEW,
          RefUpdate.Result.FORCED,
          RefUpdate.Result.NO_CHANGE,
          RefUpdate.Result.FAST_FORWARD);

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

    RefUpdateState refUpdateState = applyObject.apply(name, new RefSpec(refName), revisionsData);
    Boolean isRefUpdateSuccessful = isSuccessful(refUpdateState.getResult());

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
                  refUpdateState.getResult()));
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
              refUpdateState.getResult().name(), sourceLabel, name, refName);
      fetchStateLog.error(message);
      throw new RefUpdateException(refUpdateState.getResult(), message);
    }
    repLog.info(
        "Apply object from {} for project {}, ref name {} completed in {}ms",
        sourceLabel,
        name,
        refName,
        elapsed);
  }

  private RefFetchResult getStatus(RefUpdateState refUpdateState) {
    return isSuccessful(refUpdateState.getResult())
        ? ReplicationState.RefFetchResult.SUCCEEDED
        : ReplicationState.RefFetchResult.FAILED;
  }

  private Boolean isSuccessful(RefUpdate.Result result) {
    return SUCCESSFUL_RESULTS.contains(result);
  }

  public List<RefUpdateState> batchApplyObject(Project.NameKey name, List<RevisionInput> inputs)
      throws IOException,
          BatchRefUpdateException,
          MissingParentObjectException,
          ResourceNotFoundException,
          MissingLatestPatchSetException {

    if (inputs.isEmpty()) {
      return List.of();
    }

    // TODO: Is this the same label for all inputs?
    String sourceLabel = inputs.getFirst().getLabel();
    Timer1.Context<String> context = metrics.start(sourceLabel);

    List<RefUpdateState> refUpdateStates = applyObject.apply(name, inputs);
    boolean isRefUpdateSuccessful =
        refUpdateStates.stream().allMatch(r -> isSuccessful(r.getResult()));

    if (isRefUpdateSuccessful) {
      for (int i = 0; i < inputs.size(); i++) {
        // TODO: Ensure these have the same cardinality
        RevisionInput input = inputs.get(i);
        RefUpdateState refUpdateState = refUpdateStates.get(i);
        String refName = input.getRefName();
        long eventCreatedOn = input.getEventCreatedOn();
        RevisionData revisionData = input.getRevisionData();
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
                      refUpdateState.getResult()));
        } catch (PermissionBackendException | IllegalStateException e) {
          logger.atSevere().withCause(e).log(
              "Cannot post event for ref '%s', project %s", refName, name);
        } finally {
          Context.unsetLocalEvent();
        }
      }
    }
    long elapsed = NANOSECONDS.toMillis(context.stop());

    if (!isRefUpdateSuccessful) {
      String message =
          String.format(
              "BatchRefUpdate failed for: sourceLabel=%s, project=%s: %s",
              sourceLabel,
              name,
              refUpdateStates.stream()
                  .map(RefUpdateState::toString)
                  .collect(Collectors.joining(",")));
      fetchStateLog.error(message);
      throw new BatchRefUpdateException(
          refUpdateStates.stream().map(RefUpdateState::getResult).toList(), message);
    }
    repLog.info(
        "Batch Apply object from {} for project {}, completed in {}ms", sourceLabel, name, elapsed);
    return refUpdateStates;
  }
}

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

import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionData;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.NonFastForwardException;
import com.google.gerrit.entities.Project;
import com.google.gerrit.entities.RefNames;
import com.google.gerrit.extensions.restapi.BadRequestException;
import com.google.gerrit.extensions.restapi.PreconditionFailedException;
import com.google.gerrit.extensions.restapi.ResourceConflictException;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.RestModifyView;
import com.google.gerrit.extensions.restapi.UnprocessableEntityException;
import com.google.gerrit.server.project.ProjectResource;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Singleton
class BatchApplyObjectAction implements RestModifyView<ProjectResource, List<RevisionInput>> {
  private static final String INVOCATION_LABEL = "Batch Apply object";

  private final BatchApplyObjectCommand batchApplyObjectCommand;
  private final BatchApplyObjectInputValidator inputValidator;

  @Inject
  BatchApplyObjectAction(
      BatchApplyObjectCommand batchApplyObjectCommand,
      BatchApplyObjectInputValidator inputValidator) {
    this.batchApplyObjectCommand = batchApplyObjectCommand;
    this.inputValidator = inputValidator;
  }

  @Override
  public Response<?> apply(ProjectResource resource, List<RevisionInput> inputs)
      throws RestApiException {
    Project.NameKey projectNameKey = resource.getNameKey();

    if (inputs == null || inputs.isEmpty()) {
      throw new BadRequestException(INVOCATION_LABEL + " input cannot be null or empty");
    }

    for (RevisionInput input : inputs) {
      if (input == null) {
        throw new BadRequestException(INVOCATION_LABEL + " input cannot contain null entries");
      }
      inputValidator.validate(projectNameKey, input);
    }

    List<String> labels = inputs.stream().map(RevisionInput::getLabel).distinct().toList();
    List<Long> eventCreatedOns =
        inputs.stream().map(RevisionInput::getEventCreatedOn).distinct().toList();

    if (labels.size() != 1) {
      throw new BadRequestException(
          String.format(
              "Label for %s was expected to be exactly 1, but %s were found: [%s]",
              INVOCATION_LABEL, labels.size(), String.join(",", labels)));
    }

    if (eventCreatedOns.size() != 1) {
      throw new BadRequestException(
          String.format(
              "eventCreatedOn for %s was expected to be exactly 1, but %s were found: [%s]",
              INVOCATION_LABEL,
              eventCreatedOns.size(),
              eventCreatedOns.stream().map(Object::toString).collect(Collectors.joining(","))));
    }

    List<String> refNames = inputs.stream().map(RevisionInput::getRefName).toList();
    List<RevisionData[]> revisionData =
        inputs.stream().map(r -> new RevisionData[] {r.getRevisionData()}).toList();
    String label = labels.getFirst();
    Long eventCreatedOn = eventCreatedOns.getFirst();

    String revisionsForLog =
        inputs.stream().map(r -> r.getRevisionData().toString()).collect(Collectors.joining(","));

    try {
      batchApplyObjectCommand.batchApplyObjects(
          INVOCATION_LABEL, projectNameKey, refNames, revisionData, label, eventCreatedOn);

      return Response.ok();
    } catch (MissingParentObjectException | NonFastForwardException e) {
      if (e instanceof NonFastForwardException
          && RefNames.isRefsDraftsComments(((NonFastForwardException) e).getRefName())) {
        repLog.info(
            "{} API *REJECTED* from {} for {}:{} - {}",
            INVOCATION_LABEL,
            label,
            projectNameKey,
            refNames,
            revisionsForLog);
        throw new UnprocessableEntityException(e.getMessage());
      }
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          INVOCATION_LABEL,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw new ResourceConflictException(e.getMessage(), e);
    } catch (NumberFormatException | IOException e) {
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          INVOCATION_LABEL,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw RestApiException.wrap(e.getMessage(), e);
    } catch (BatchRefUpdateException e) {
      if (refNames.stream().anyMatch(RefNames::isRefsDraftsComments)
          && e.containsRejectedResult()) {
        repLog.info(
            "{} API *REJECTED* from {} for {}:{} - {}",
            INVOCATION_LABEL,
            label,
            projectNameKey,
            refNames,
            revisionsForLog);
      } else {
        repLog.error(
            "{} API *FAILED* from {} for {}:{} - {}",
            INVOCATION_LABEL,
            label,
            projectNameKey,
            refNames,
            revisionsForLog,
            e);
      }
      throw new UnprocessableEntityException(e.getMessage());
    } catch (MissingLatestPatchSetException e) {
      repLog.error(
          "{} API *FAILED* from {} for {}:{} - {}",
          INVOCATION_LABEL,
          label,
          projectNameKey,
          refNames,
          revisionsForLog,
          e);
      throw new PreconditionFailedException(e.getMessage());
    }
  }
}

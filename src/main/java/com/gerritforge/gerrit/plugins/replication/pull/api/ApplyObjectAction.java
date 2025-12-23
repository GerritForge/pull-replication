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

import static com.gerritforge.gerrit.plugins.replication.pull.PullReplicationLogger.repLog;

import com.gerritforge.gerrit.plugins.replication.pull.SourcesCollection;
import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.BatchRefUpdateException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingLatestPatchSetException;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.MissingParentObjectException;
import com.google.gerrit.entities.RefNames;
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

@Singleton
public class ApplyObjectAction implements RestModifyView<ProjectResource, RevisionInput> {

  private final ApplyObjectCommand applyObjectCommand;
<<<<<<< PATCH SET (2bcf140a5c6535c60ecb31c98ae543b25b7c79ec Extract input validation and add ApplyObjectCommand batch ap)
  private final ApplyObjectInputValidator inputValidator;
=======
  private final FetchPreconditions preConditions;
  private final SourcesCollection sourcesCollection;
>>>>>>> BASE      (adf3aea6915002a7e18d89378952f31af6bf18a3 Add ApplyObject.applyBatch for multi-ref BatchRefUpdate exec)

  @Inject
  public ApplyObjectAction(
<<<<<<< PATCH SET (2bcf140a5c6535c60ecb31c98ae543b25b7c79ec Extract input validation and add ApplyObjectCommand batch ap)
      ApplyObjectCommand applyObjectCommand, ApplyObjectInputValidator inputValidator) {
=======
      ApplyObjectCommand applyObjectCommand,
      FetchPreconditions preConditions,
      SourcesCollection sourcesCollection) {
>>>>>>> BASE      (adf3aea6915002a7e18d89378952f31af6bf18a3 Add ApplyObject.applyBatch for multi-ref BatchRefUpdate exec)
    this.applyObjectCommand = applyObjectCommand;
<<<<<<< PATCH SET (2bcf140a5c6535c60ecb31c98ae543b25b7c79ec Extract input validation and add ApplyObjectCommand batch ap)
    this.inputValidator = inputValidator;
=======
    this.preConditions = preConditions;
    this.sourcesCollection = sourcesCollection;
>>>>>>> BASE      (adf3aea6915002a7e18d89378952f31af6bf18a3 Add ApplyObject.applyBatch for multi-ref BatchRefUpdate exec)
  }

  @Override
  public Response<?> apply(ProjectResource resource, RevisionInput input) throws RestApiException {

<<<<<<< PATCH SET (2bcf140a5c6535c60ecb31c98ae543b25b7c79ec Extract input validation and add ApplyObjectCommand batch ap)
    inputValidator.validate(resource.getNameKey(), input);

    repLog.info(
        "Apply object API from {} for {}:{} - {}",
        input.getLabel(),
        resource.getNameKey(),
        input.getRefName(),
        input.getRevisionData());
=======
    if (!preConditions.canCallFetchApi()) {
      throw new AuthException("Not allowed to call fetch command");
    }
    if (Strings.isNullOrEmpty(input.getLabel())) {
      throw new BadRequestException("Source label cannot be null or empty");
    }
    if (sourcesCollection.getByRemoteName(input.getLabel()).isEmpty()) {
      throw new BadRequestException(
          "Source label " + input.getLabel() + " is not a configured remote");
    }
    if (Strings.isNullOrEmpty(input.getRefName())) {
      throw new BadRequestException("Ref-update refname cannot be null or empty");
    }
    if (Objects.isNull(input.getRevisionData())) {
      throw new BadRequestException("Revision data cannot be null");
    }
>>>>>>> BASE      (adf3aea6915002a7e18d89378952f31af6bf18a3 Add ApplyObject.applyBatch for multi-ref BatchRefUpdate exec)

    try {
      applyObjectCommand.applyObject(
          resource.getNameKey(),
          input.getRefName(),
          input.getRevisionData(),
          input.getLabel(),
          input.getEventCreatedOn());
      return Response.created();
    } catch (MissingParentObjectException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          resource.getNameKey(),
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw new ResourceConflictException(e.getMessage(), e);
    } catch (NumberFormatException | IOException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          resource.getNameKey(),
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw RestApiException.wrap(e.getMessage(), e);
    } catch (BatchRefUpdateException e) {
      if (RefNames.isRefsDraftsComments(input.getRefName()) && e.containsRejectedResult()) {
        repLog.info(
            "Apply object API *REJECTED* from {} for {}:{} - {}",
            input.getLabel(),
            resource.getNameKey(),
            input.getRefName(),
            input.getRevisionData());
      } else {
        repLog.error(
            "Apply object API *FAILED* from {} for {}:{} - {}",
            input.getLabel(),
            resource.getNameKey(),
            input.getRefName(),
            input.getRevisionData(),
            e);
      }
      throw new UnprocessableEntityException(e.getMessage());
    } catch (MissingLatestPatchSetException e) {
      repLog.error(
          "Apply object API *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          resource.getNameKey(),
          input.getRefName(),
          input.getRevisionData(),
          e);
      throw new PreconditionFailedException(e.getMessage());
    }
  }
}

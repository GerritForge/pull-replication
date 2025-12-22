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
  private final ApplyObjectInputValidator inputValidator;

  @Inject
  public ApplyObjectAction(
      ApplyObjectCommand applyObjectCommand, ApplyObjectInputValidator inputValidator) {
    this.applyObjectCommand = applyObjectCommand;
    this.inputValidator = inputValidator;
  }

  @Override
  public Response<?> apply(ProjectResource resource, RevisionInput input) throws RestApiException {

    inputValidator.validate(resource.getNameKey(), input);

    repLog.info(
        "Apply object API from {} for {}:{} - {}",
        input.getLabel(),
        resource.getNameKey(),
        input.getRefName(),
        input.getRevisionData());

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

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
import static com.google.common.base.Strings.isNullOrEmpty;

import com.gerritforge.gerrit.plugins.replication.pull.api.data.RevisionInput;
import com.google.gerrit.entities.Project;
import com.google.gerrit.extensions.restapi.AuthException;
import com.google.gerrit.extensions.restapi.BadRequestException;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class ApplyObjectInputValidator {
  private final FetchPreconditions preConditions;

  @Inject
  ApplyObjectInputValidator(FetchPreconditions preConditions) {
    this.preConditions = preConditions;
  }

  void validate(Project.NameKey projectNameKey, RevisionInput input)
      throws AuthException, BadRequestException {
    if (!preConditions.canCallFetchApi()) {
      throw new AuthException("Not allowed to call fetch command");
    }
    if (isNullOrEmpty(input.getLabel())) {
      throw new BadRequestException("Source label cannot be null or empty");
    }
    if (isNullOrEmpty(input.getRefName())) {
      throw new BadRequestException("Ref-update refname cannot be null or empty");
    }
    if (input.getRevisionData() == null) {
      throw new BadRequestException("Revision data cannot be null");
    }

    try {
      input.validate();
    } catch (IllegalArgumentException e) {
      BadRequestException bre =
          new BadRequestException("Ref-update with invalid input: " + e.getMessage(), e);
      repLog.error(
          "Validation *FAILED* from {} for {}:{} - {}",
          input.getLabel(),
          projectNameKey,
          input.getRefName(),
          input.getRevisionData(),
          bre);
      throw bre;
    }
  }
}

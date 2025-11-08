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

import com.google.common.flogger.FluentLogger;
import com.google.gerrit.entities.Project;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.gerritforge.gerrit.plugins.replication.pull.api.FetchAction.BatchInput;
import com.gerritforge.gerrit.plugins.replication.pull.api.exception.RemoteConfigurationMissingException;
import org.eclipse.jgit.errors.TransportException;

public class FetchJob implements Runnable {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();

  public interface Factory {
    FetchJob create(
        Project.NameKey project, BatchInput input, PullReplicationApiRequestMetrics metrics);
  }

  private FetchCommand command;
  private Project.NameKey project;
  private BatchInput batchInput;
  private final PullReplicationApiRequestMetrics metrics;

  @Inject
  public FetchJob(
      FetchCommand command,
      @Assisted Project.NameKey project,
      @Assisted BatchInput batchInput,
      @Assisted PullReplicationApiRequestMetrics metrics) {
    this.command = command;
    this.project = project;
    this.batchInput = batchInput;
    this.metrics = metrics;
  }

  @Override
  public void run() {
    try {
      command.fetchAsync(project, batchInput.label, batchInput.getRefSpecs(), metrics);
    } catch (InterruptedException | RemoteConfigurationMissingException | TransportException e) {
      log.atSevere().withCause(e).log(
          "Exception during the async fetch call for project %s, label %s and ref(s) name(s) %s",
          project.get(), batchInput.label, batchInput.getRefSpecs());
    }
  }
}

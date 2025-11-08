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

package com.gerritforge.gerrit.plugins.replication.pull;

import com.google.gerrit.extensions.api.changes.NotifyHandling;
import com.google.gerrit.extensions.events.HeadUpdatedListener;

class FakeHeadUpdateEvent implements HeadUpdatedListener.Event {

  private final String oldName;
  private final String newName;
  private final String projectName;

  FakeHeadUpdateEvent(String oldName, String newName, String projectName) {

    this.oldName = oldName;
    this.newName = newName;
    this.projectName = projectName;
  }

  @Override
  public NotifyHandling getNotify() {
    return NotifyHandling.NONE;
  }

  @Override
  public String getOldHeadName() {
    return oldName;
  }

  @Override
  public String getNewHeadName() {
    return newName;
  }

  @Override
  public String getProjectName() {
    return projectName;
  }
}

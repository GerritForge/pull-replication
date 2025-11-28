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

package com.gerritforge.gerrit.plugins.replication.pull.api.data;

import java.util.Arrays;

public class RevisionsInput {
  private String label;

  private String refName;

  private long eventCreatedOn;
  private RevisionData[] revisionsData;
  private boolean storeRefLog;

  private boolean storeRefLog;

  public RevisionsInput(
      String label,
      String refName,
      long eventCreatedOn,
      RevisionData[] revisionsData,
      boolean storeRefLog) {
    this.label = label;
    this.refName = refName;
    this.eventCreatedOn = eventCreatedOn;
    this.revisionsData = revisionsData;
    this.storeRefLog = storeRefLog;
  }

  public String getLabel() {
    return label;
  }

  public String getRefName() {
    return refName;
  }

  public long getEventCreatedOn() {
    return eventCreatedOn;
  }

  public RevisionData[] getRevisionsData() {
    return revisionsData;
  }

<<<<<<< PATCH SET (770bfd900235e685fe4fd31956966f90efb0c2a7 Use the new remote.NAME.storeRefLog for creating new reposit)
  public boolean storeRefLog() {
=======
  public boolean isStoreRefLog() {
>>>>>>> BASE      (0810f99e1b8f146d3eee81be7da497d02b0eaca2 Adapt pull-replication to support explicit project exclusion)
    return storeRefLog;
  }

  public void validate() {
    for (RevisionData revisionData : revisionsData) {
      RevisionInput.validate(refName, revisionData);
    }
  }

  @Override
  public String toString() {
    return "RevisionsInput { "
        + label
        + ":"
        + refName
        + " - "
        + Arrays.toString(revisionsData)
        + ","
        + "storeRefLog = "
<<<<<<< PATCH SET (770bfd900235e685fe4fd31956966f90efb0c2a7 Use the new remote.NAME.storeRefLog for creating new reposit)
        + storeRefLog
=======
        + isStoreRefLog()
>>>>>>> BASE      (0810f99e1b8f146d3eee81be7da497d02b0eaca2 Adapt pull-replication to support explicit project exclusion)
        + "}";
  }
}

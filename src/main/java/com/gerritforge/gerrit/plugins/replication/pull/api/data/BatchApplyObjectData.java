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

import com.google.auto.value.AutoValue;
import java.util.Optional;

@AutoValue
public abstract class BatchApplyObjectData {

  public static BatchApplyObjectData create(
      String refName, Optional<RevisionData> revisionData, boolean isDelete, boolean isCreate)
      throws IllegalArgumentException {
    return new AutoValue_BatchApplyObjectData(refName, revisionData, isDelete, isCreate);
  }

  public static BatchApplyObjectData newDeleteRef(String refName) throws IllegalArgumentException {
    return create(refName, Optional.empty(), true, false);
  }

  public static BatchApplyObjectData newUpdateRef(
      String refName, Optional<RevisionData> revisionData) throws IllegalArgumentException {
    return create(refName, revisionData, false, false);
  }

  public static BatchApplyObjectData newCreateRef(
      String refName, Optional<RevisionData> revisionData) throws IllegalArgumentException {
    return create(refName, revisionData, false, true);
  }

  public abstract String refName();

  public abstract Optional<RevisionData> revisionData();

  public abstract boolean isDelete();

  public abstract boolean isCreate();

  @Override
  public String toString() {
    return String.format(
        "%s:%s isDelete=%s",
        refName(), revisionData().map(RevisionData::toString).orElse("ABSENT"), isDelete());
  }
}

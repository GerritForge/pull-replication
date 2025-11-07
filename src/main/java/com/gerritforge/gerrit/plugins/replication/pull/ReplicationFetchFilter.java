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

import com.google.gerrit.extensions.annotations.ExtensionPoint;
import com.gerritforge.gerrit.plugins.replication.pull.FetchOne.LockFailureException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filter that is invoked before a set of remote refs are fetched from a remote instance.
 *
 * <p>It can be used to filter out unwanted fetches.
 */
@ExtensionPoint
public interface ReplicationFetchFilter {

  Set<String> filter(String projectName, Set<String> fetchRefs);

  default Map<String, AutoCloseable> filterAndLock(String projectName, Set<String> fetchRefs)
      throws LockFailureException {
    return filter(projectName, fetchRefs).stream()
        .collect(Collectors.toMap(ref -> ref, ref -> () -> {}));
  }
}

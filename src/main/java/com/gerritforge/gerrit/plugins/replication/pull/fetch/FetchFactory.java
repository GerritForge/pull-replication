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

package com.gerritforge.gerrit.plugins.replication.pull.fetch;

import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.URIish;

public interface FetchFactory {
  Fetch create(String taskIdHex, URIish uri, Repository git);

  // Return implementation without any decorators
  @FetchClientImplementation
  Fetch createPlainImpl(String taskIdHex, URIish uri, Repository git);
}

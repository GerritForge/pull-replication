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

import com.google.gerrit.extensions.registration.DynamicItem;
import com.google.inject.AbstractModule;

/**
 * Gerrit libModule for applying a fetch-filter for pull replications.
 *
 * <p>It should be used only when an actual filter is defined, otherwise the default plugin
 * behaviour will be fetching refs without any filtering.
 */
public class ReplicationExtensionPointModule extends AbstractModule {

  @Override
  protected void configure() {
    DynamicItem.itemOf(binder(), ReplicationFetchFilter.class);
  }
}
